const dayjs = require('dayjs');
const prettyMs = require('pretty-ms');

const defaultStepUnitConverter = (value) => `${value}r`;

class ProgressReporter {
    progressTracker = null;
    lastReport = null;
    reportInterval = null;
    outputs = [];
    pretty = null;
    stepUnitConverter = null;

    constructor(progressTracker, reportInterval = 2000, pretty = true, stepUnitConverter = defaultStepUnitConverter) {
        this.progressTracker = progressTracker;
        this.reportInterval = reportInterval;
        this.pretty = pretty;
        this.stepUnitConverter = stepUnitConverter;
    }
    addOutput(output) {
        this.outputs.push(output);
    }
    report(force = false) {
        if (this.lastReport == null || (new Date().getTime() - this.lastReport) > this.reportInterval || force) {
            if (this.outputs.length > 0) {
                let pretty = null;
                if (this.pretty) {
                    pretty = {
                        id: this.progressTracker.id,
                        state: {
                            status: this.progressTracker.state.status,
                            total: this.progressTracker.state.total == null ? 'unknown' : this.stepUnitConverter(this.progressTracker.state.total),
                            start: this.progressTracker.state.start == null ? 'unknown' : dayjs(this.progressTracker.state.start).toDate().toLocaleString(),
                            finish: this.progressTracker.state.finish == null ? 'unknown' : dayjs(this.progressTracker.state.finish).toDate().toLocaleString(),
                            elapsed: prettyMs(this.progressTracker.state.elapsed),
                            current: this.progressTracker.state.current == null ? 'unknown' : this.stepUnitConverter(this.progressTracker.state.current),
                            percentage: this.progressTracker.state.percentage == null ? 'unknown' : parseFloat(this.progressTracker.state.percentage.toFixed(2)),
                            percentageBar: this.progressTracker.state.percentage == null ? null : '▇'.repeat(this.progressTracker.state.percentage / 2) + '-'.repeat(100 / 2 - this.progressTracker.state.percentage / 2),
                            eta: this.progressTracker.state.eta == null ? 'unknown' : prettyMs(this.progressTracker.state.eta),
                            speed: this.progressTracker.state.speed == null ? 'unknown' : this.stepUnitConverter(parseFloat(this.progressTracker.state.speed.toFixed(2))) + '/s'
                        }
                    };
                }
                this.outputs.forEach(output => output.write(this.progressTracker, pretty));
            }
            this.lastReport = new Date().getTime();
        }
    }
}

module.exports = ProgressReporter;
