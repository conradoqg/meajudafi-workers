const ChainableError = require('../util/chainableError').Error;

class WorkerError extends ChainableError {    
    constructor(...args) {                        
        super(...args);        
        this.name = this.constructor.name;
    }
}

module.exports = WorkerError;