const glob = (() => {
    try {
        return window;
    }
    catch (e) {
        // @ts-ignore-next-line
        return global;
    }
})();
const isErrorExtensible = (() => {
    try {
        // making sure this is an js engine which creates "extensible" error stacks (i.e. not firefox)
        const stack = new glob.Error('Test String').stack;
        return stack.slice(0, 26) == 'Error: Test String\n    at ';
    }
    catch (e) {
        return false;
    }
})();
exports.OriginalError = glob.Error;
let ff = v => JSON.stringify(v, undefined, 4);
exports.getFormatFunction = () => ff;
exports.setFormatFunction = (newFormatFunction) => {
    if (typeof newFormatFunction != 'function')
        throw new TypeError('the function for formatFunction has to by of type function but was ' +
            newFormatFunction);
    ff = newFormatFunction;
};
const formatForOutput = (v) => {
    try {
        return ff(v).replace(/\n/g, '\n    ');
    }
    catch (e) {
        return '' + v;
    }
};
const chainErrorsFunction = (e1, e2) => {
    if (e1 instanceof exports.OriginalError)
        e2.stack += '\nCaused by: ' + e1.stack;
    else
        e2.stack += '\nWas caused by throwing:\n    ' + formatForOutput(e1);
    return e2;
};
exports.chainErrors = !isErrorExtensible
    ? (e1, e2) => e2
    : chainErrorsFunction;
class Error extends exports.OriginalError {
    constructor(msg, chained) {
        super(msg);
        this.name = this.constructor.name;
        if (arguments.length > 1)
            exports.chainErrors(chained, this);
    }
}
exports.Error = !isErrorExtensible
    ? exports.OriginalError
    : Error;
exports.replaceChainedWithOriginal = !isErrorExtensible
    ? () => { }
    : () => {
        if (glob.Error == exports.Error)
            glob.Error = exports.OriginalError;
    };
exports.replaceOriginalWithChained = !isErrorExtensible
    ? () => { }
    : () => {
        if (glob.Error != exports.Error)
            glob.Error = exports.Error;
    };
