/*
 * hdcs engine
 *
 * IO engine using Intel's libhdcs.
 *
 */

#include <hdcs/libhdcs.h>

#include "../fio.h"
#include "../optgroup.h"

struct fio_hdcs_iou {
	struct io_u *io_u;
	hdcs_completion_t completion;
	int io_seen;
	int io_complete;
};

struct hdcs_data {
  hdcs_ioctx_t io;
	struct io_u **aio_events;
	struct io_u **sort_events;
};

struct hdcs_options {
	void *pad;
	char *cluster_name;
	char *hdcs_name;
	char *pool_name;
	char *client_name;
	int busy_poll;
};

static struct fio_option options[] = {
};

static int _fio_setup_hdcs_data(struct thread_data *td,
			       struct hdcs_data **hdcs_data_ptr)
{
	struct hdcs_data *hdcs;

	if (td->io_ops->data)
		return 0;

	hdcs = calloc(1, sizeof(struct hdcs_data));
	if (!hdcs)
		goto failed;

	hdcs->aio_events = calloc(td->o.iodepth, sizeof(struct io_u *));
	if (!hdcs->aio_events)
		goto failed;

	hdcs->sort_events = calloc(td->o.iodepth, sizeof(struct io_u *));
	if (!hdcs->sort_events)
		goto failed;

	*hdcs_data_ptr = hdcs;
	return 0;

failed:
	if (hdcs)
		free(hdcs);
	return 1;

}

static int _fio_hdcs_connect(struct thread_data *td)
{
	struct hdcs_data *hdcs = td->io_ops->data;

	hdcs_open(&(hdcs->io));
	return 0;

}

static void _fio_hdcs_disconnect(struct hdcs_data *hdcs)
{
	if (!hdcs)
		return;

	/* shutdown everything */
  hdcs_close(hdcs->io);
}

static void _fio_hdcs_finish_aiocb(hdcs_completion_t comp, void *data)
{
	struct fio_hdcs_iou *fri = data;
	struct io_u *io_u = fri->io_u;
	ssize_t ret;

	/*
	 * Looks like return value is 0 for success, or < 0 for
	 * a specific error. So we have to assume that it can't do
	 * partial completions.
	 */
	ret = hdcs_aio_get_return_value(fri->completion);
	if (ret < 0) {
		io_u->error = ret;
		io_u->resid = io_u->xfer_buflen;
	} else
		io_u->error = 0;

	fri->io_complete = 1;
}

static struct io_u *fio_hdcs_event(struct thread_data *td, int event)
{
	struct hdcs_data *hdcs = td->io_ops->data;

	return hdcs->aio_events[event];
}

static inline int fri_check_complete(struct hdcs_data *hdcs, struct io_u *io_u,
				     unsigned int *events)
{
	struct fio_hdcs_iou *fri = io_u->engine_data;

	if (fri->io_complete) {
		fri->io_seen = 1;
		hdcs->aio_events[*events] = io_u;
		(*events)++;

		hdcs_aio_release(fri->completion);
		return 1;
	}

	return 0;
}

static inline int hdcs_io_u_seen(struct io_u *io_u)
{
	struct fio_hdcs_iou *fri = io_u->engine_data;

	return fri->io_seen;
}

static void hdcs_io_u_wait_complete(struct io_u *io_u)
{
	struct fio_hdcs_iou *fri = io_u->engine_data;

	hdcs_aio_wait_for_complete(fri->completion);
}

static int hdcs_io_u_cmp(const void *p1, const void *p2)
{
	const struct io_u **a = (const struct io_u **) p1;
	const struct io_u **b = (const struct io_u **) p2;
	uint64_t at, bt;

	at = utime_since_now(&(*a)->start_time);
	bt = utime_since_now(&(*b)->start_time);

	if (at < bt)
		return -1;
	else if (at == bt)
		return 0;
	else
		return 1;
}

static int hdcs_iter_events(struct thread_data *td, unsigned int *events,
			   unsigned int min_evts, int wait)
{
	struct hdcs_data *hdcs = td->io_ops->data;
	unsigned int this_events = 0;
	struct io_u *io_u;
	int i, sidx;

	sidx = 0;
	io_u_qiter(&td->io_u_all, io_u, i) {
		if (!(io_u->flags & IO_U_F_FLIGHT))
			continue;
		if (hdcs_io_u_seen(io_u))
			continue;

		if (fri_check_complete(hdcs, io_u, events))
			this_events++;
		else if (wait)
			hdcs->sort_events[sidx++] = io_u;
	}

	if (!wait || !sidx)
		return this_events;

	/*
	 * Sort events, oldest issue first, then wait on as many as we
	 * need in order of age. If we have enough events, stop waiting,
	 * and just check if any of the older ones are done.
	 */
	if (sidx > 1)
		qsort(hdcs->sort_events, sidx, sizeof(struct io_u *), hdcs_io_u_cmp);

	for (i = 0; i < sidx; i++) {
		io_u = hdcs->sort_events[i];

		if (fri_check_complete(hdcs, io_u, events)) {
			this_events++;
			continue;
		}

		/*
		 * Stop waiting when we have enough, but continue checking
		 * all pending IOs if they are complete.
		 */
		if (*events >= min_evts)
			continue;

		hdcs_io_u_wait_complete(io_u);

		if (fri_check_complete(hdcs, io_u, events))
			this_events++;
	}

	return this_events;
}

static int fio_hdcs_getevents(struct thread_data *td, unsigned int min,
			     unsigned int max, const struct timespec *t)
{
	unsigned int this_events, events = 0;
	struct hdcs_options *o = td->eo;
	int wait = 0;

	do {
		this_events = hdcs_iter_events(td, &events, min, wait);

		if (events >= min)
			break;
		if (this_events)
			continue;

		if (!o->busy_poll)
			wait = 1;
		else
			nop;
	} while (1);

	return events;
}

static int fio_hdcs_queue(struct thread_data *td, struct io_u *io_u)
{
	struct hdcs_data *hdcs = td->io_ops->data;
	struct fio_hdcs_iou *fri = io_u->engine_data;
	int r = -1;

	fio_ro_check(td, io_u);

	fri->io_seen = 0;
	fri->io_complete = 0;

	r = hdcs_aio_create_completion(fri, _fio_hdcs_finish_aiocb,
						&fri->completion);
	if (r < 0) {
		log_err("hdcs_aio_create_completion failed.\n");
		goto failed;
	}

	if (io_u->ddir == DDIR_WRITE) {
		r = hdcs_aio_write(hdcs->io, io_u->xfer_buf, io_u->offset, io_u->xfer_buflen, fri->completion);
		if (r < 0) {
			log_err("hdcs_aio_write failed.\n");
			goto failed_comp;
		}

	} else if (io_u->ddir == DDIR_READ) {
		r = hdcs_aio_read(hdcs->io, io_u->xfer_buf, io_u->offset, io_u->xfer_buflen, fri->completion);

		if (r < 0) {
			log_err("hdcs_aio_read failed.\n");
			goto failed_comp;
		}
	} else if (io_u->ddir == DDIR_TRIM) {
		/*r = hdcs_aio_discard(io_u->offset,
					io_u->xfer_buflen, fri->completion);
		if (r < 0) {
			log_err("hdcs_aio_discard failed.\n");
			goto failed_comp;
		}*/
	} else if (io_u->ddir == DDIR_SYNC) {
		/*r = hdcs_aio_flush(hdcs->image, fri->completion);
		if (r < 0) {
			log_err("hdcs_flush failed.\n");
			goto failed_comp;
		}*/
	} else {
		dprint(FD_IO, "%s: Warning: unhandled ddir: %d\n", __func__,
		       io_u->ddir);
		goto failed_comp;
	}

	return FIO_Q_QUEUED;
failed_comp:
	hdcs_aio_release(fri->completion);
failed:
	io_u->error = r;
	td_verror(td, io_u->error, "xfer");
	return FIO_Q_COMPLETED;
}

static int fio_hdcs_init(struct thread_data *td)
{
	int r;

	r = _fio_hdcs_connect(td);
	if (r) {
		log_err("fio_hdcs_connect failed, return code: %d .\n", r);
		goto failed;
	}

	return 0;

failed:
	return 1;
}

static void fio_hdcs_cleanup(struct thread_data *td)
{
	struct hdcs_data *hdcs = td->io_ops->data;

	if (hdcs) {
		_fio_hdcs_disconnect(hdcs);
		free(hdcs->aio_events);
		free(hdcs->sort_events);
		free(hdcs);
	}
}

static int fio_hdcs_setup(struct thread_data *td)
{
	//hdcs_image_info_t info;
	struct fio_file *f;
	struct hdcs_data *hdcs = NULL;
	//int major, minor, extra;
	int r;

	/* log version of libhdcs. No cluster connection required. */
	/*hdcs_version(&major, &minor, &extra);
	log_info("hdcs engine: RBD version: %d.%d.%d\n", major, minor, extra);
  */
	/* allocate engine specific structure to deal with libhdcs. */
	r = _fio_setup_hdcs_data(td, &hdcs);
	if (r) {
		log_err("fio_setup_hdcs_data failed.\n");
		goto cleanup;
	}
	td->io_ops->data = hdcs;

	/* libhdcs does not allow us to run first in the main thread and later
	 * in a fork child. It needs to be the same process context all the
	 * time. 
	 */
	td->o.use_thread = 1;

	/* connect in the main thread to determine to determine
	 * the size of the given RADOS block device. And disconnect
	 * later on.
	 */
	/*r = _fio_hdcs_connect(td);
	if (r) {
		log_err("fio_hdcs_connect failed.\n");
		goto cleanup;
	}*/

	/* get size of the RADOS block device */
	/*r = hdcs_stat(hdcs->image, &info, sizeof(info));
	if (r < 0) {
		log_err("hdcs_status failed.\n");
		goto disconnect;
	}
	dprint(FD_IO, "hdcs-engine: image size: %lu\n", info.size);
  */
	/* taken from "net" engine. Pretend we deal with files,
	 * even if we do not have any ideas about files.
	 * The size of the RBD is set instead of a artificial file.
	 */
	if (!td->files_index) {
		add_file(td, td->o.filename ? : "hdcs", 0, 0);
		td->o.nr_files = td->o.nr_files ? : 1;
		td->o.open_files++;
	}
	f = td->files[0];
	f->real_file_size = 1073741824;

	/* disconnect, then we were only connected to determine
	 * the size of the RBD.
	 */
	/*_fio_hdcs_disconnect(hdcs);*/
	return 0;

cleanup:
	fio_hdcs_cleanup(td);
	return r;
}

static int fio_hdcs_open(struct thread_data *td, struct fio_file *f)
{
	return 0;
}

static int fio_hdcs_invalidate(struct thread_data *td, struct fio_file *f)
{
	return 0;
}

static void fio_hdcs_io_u_free(struct thread_data *td, struct io_u *io_u)
{
	struct fio_hdcs_iou *fri = io_u->engine_data;

	if (fri) {
		io_u->engine_data = NULL;
		free(fri);
	}
}

static int fio_hdcs_io_u_init(struct thread_data *td, struct io_u *io_u)
{
	struct fio_hdcs_iou *fri;

	fri = calloc(1, sizeof(*fri));
	fri->io_u = io_u;
	io_u->engine_data = fri;
	return 0;
}

static struct ioengine_ops ioengine = {
	.name			= "hdcs",
	.version		= FIO_IOOPS_VERSION,
	.setup			= fio_hdcs_setup,
	.init			= fio_hdcs_init,
	.queue			= fio_hdcs_queue,
	.getevents		= fio_hdcs_getevents,
	.event			= fio_hdcs_event,
	.cleanup		= fio_hdcs_cleanup,
	.open_file		= fio_hdcs_open,
	.invalidate		= fio_hdcs_invalidate,
	.options		= options,
	.io_u_init		= fio_hdcs_io_u_init,
	.io_u_free		= fio_hdcs_io_u_free,
	.option_struct_size	= sizeof(struct hdcs_options),
};

static void fio_init fio_hdcs_register(void)
{
	register_ioengine(&ioengine);
}

static void fio_exit fio_hdcs_unregister(void)
{
	unregister_ioengine(&ioengine);
}
