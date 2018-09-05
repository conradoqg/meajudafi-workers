const Progress = require('./progress');

class UI {
    constructor() {
        this.progressStates = [];
        this.progress = new Progress();
    }

    start(id, progressTemplate, finishTemplate) {
        this.progress.start('Downloading', false);
        this.progressStates.push({ id, state: null, progressTemplate, finishTemplate });
    }

    update(id, progress) {
        const item = this.progressStates.find(item => item.id == id);
        if (item) {
            item.state = Object.assign(item.state || {}, progress);
            const text = this.progressStates.filter(item => item.state).map(item => item.progressTemplate(item.state)).join('\n') + '\n';
            this.progress.tick(text);
        }
    }

    stop(id) {
        const item = this.progressStates.find(item => item.id == id);
        if (item) {
            this.progress.stop(item.finishTemplate ? item.finishTemplate(item.state) : item.progressTemplate(item.state));
            this.progressStates = this.progressStates.filter(item => item.id !== id);
        }
    }

    close() {
        this.progress.close();
    }
}

module.exports = UI;