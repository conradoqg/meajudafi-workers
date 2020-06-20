class ProgressLogger {
    progressTracker = null;

    constructor(progressTracker) {
        this.progressTracker = progressTracker;
    }


    log(text) {
        console.log(`${this.progressTracker.id}: ${text}`);
    }
}

module.exports = ProgressLogger;