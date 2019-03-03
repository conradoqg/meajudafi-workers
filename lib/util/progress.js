const inquirer = require('inquirer');

class Progress {
    constructor() {
        this.ui = new inquirer.ui.BottomBar();
        this.loader = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
        this.loaderTick = 4;
    }

    static now(unit) {

        const hrTime = process.hrtime();
        switch (unit) {
            case 'milli': return hrTime[0] * 1000 + hrTime[1] / 1000000;
            case 'micro': return hrTime[0] * 1000000 + hrTime[1] / 1000;
            case 'nano': return hrTime[0] * 1000000000 + hrTime[1];
            default: return hrTime[0] * 1000000000 + hrTime[1];
        }

    }

    start(text, indertermined = true) {
        if (indertermined) {
            this.interval = setInterval(() => {
                this.ui.updateBottomBar(this.loader[this.loaderTick++ % this.loader.length] + ' ' + text);
            }, 300);
        } else {            
            this.ui.updateBottomBar(text);
        }
    }

    tick(text) {
        if (!this.lastUpdate || Progress.now('milli') - this.lastUpdate > 5000) {
            this.ui.updateBottomBar(text);
            this.lastUpdate = Progress.now('milli');
        }
    }

    stop(text, leave = false) {
        if (this.interval) {
            clearInterval(this.interval);
            this.interval = null;
        }
        this.lastUpdate = null;
        if (!leave) this.ui.updateBottomBar('');
        else process.stdout.write('\n');        
        process.stdout.write(text + '\n');
    }

    close() {
        this.ui.close();

        if (this.interval) {
            clearInterval(this.interval);
            this.interval = null;
        }
    }
}

module.exports = Progress;