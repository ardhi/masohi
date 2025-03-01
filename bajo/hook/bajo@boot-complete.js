import jobQueueWorker from '../../lib/job-queue-worker.js'

async function bootComplete () {
  if (!this.jobQueue || this.app.bajo.config.applet) return
  if (!this.config.jobQueue.localWorker) return
  jobQueueWorker.call(this, this.jobQueue)
}

export default bootComplete
