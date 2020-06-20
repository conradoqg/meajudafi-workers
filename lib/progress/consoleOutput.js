class ConsoleOutput {
    template = null;
    constructor(template) {
        this.template = template;
    }
    write(progressTracker, prettyProgressTracker) {
        if (this.template)
            console.log(this.template(progressTracker, prettyProgressTracker));
        else
            console.log(JSON.stringify(prettyProgressTracker));
    }
}
module.exports = ConsoleOutput;
