class ProgressTracker {
    id = null;
    state = {
        status: 'new',
        total: null,
        start: null,
        finish: null,
        elapsed: 0,
        current: null,
        percentage: null,
        eta: null,
        speed: null
    }

    constructor(id) {
        this.id = id;
    }

    start(total) {
        if (this.state.status != 'running') {
            this.state.total = total;
            this.state.start = new Date().getTime();
            this.state.finish = null;
            this.state.elapsed = 0;
            this.state.current = total > 0 ? 0 : null;
            this.state.percentage = total > 0 ? 0 : null;
            this.state.eta = null;
            this.state.speed = null;
            this.state.status = 'running';
        } else {
            throw new Error('Progress is already running');
        }
    }

    error() {
        if (this.state.status == 'running') {
            this.recalculateTimmings();
            this.state.finish = new Date().getTime();
            this.state.status = 'errored';
        } else {
            throw new Error('Progress is not running to be errored');
        }
    }

    end() {
        if (this.state.status == 'running') {
            this.recalculateTimmings();
            this.state.finish = new Date().getTime();
            this.state.status = 'ended';
        } else {
            throw new Error('Progress is not running to be ended');
        }
    }

    step(stepAmount = 1) {
        if (this.state.status == 'running') {
            if (this.state.total > 0) {
                if ((this.state.current + stepAmount) <= this.state.total) {
                    this.state.current += stepAmount;
                    this.state.percentage = (this.state.current * 100) / this.state.total;
                } else {
                    throw new Error(`There are more steps (${this.state.current + stepAmount}) than the expected (${this.state.total})`);
                }
            }
            this.recalculateTimmings();
        } else {
            throw new Error('Progress is not running to be stepped');
        }
    }

    recalculateTimmings() {
        this.state.elapsed = new Date().getTime() - this.state.start;
        if (this.state.total > 0) {
            this.state.speed = this.state.elapsed > 0 ? (this.state.current / this.state.elapsed) * 1000 : null;
            this.state.eta = this.state.current > 0 ? ((this.state.elapsed * this.state.total) / this.state.current) - this.state.elapsed : null;
        }
    }
}

module.exports = ProgressTracker;