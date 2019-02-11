// A standard deviation object constructor. Running deviation (avoid growing arrays) which
// is round-off error resistant. Based on an algorithm found in a Knuth book.
function StandardDeviation(firstMeasurement) {
    this.workData = firstMeasurement == null ? 0 : firstMeasurement;
    this.lastWorkData = firstMeasurement == null ? null : firstMeasurement;
    this.S = 0;
    this.count = firstMeasurement == null ? 0 : 1;
}

// Add a measurement. Also calculates updates to stepwise parameters which are later used
// to determine sigma.
StandardDeviation.prototype.addMeasurement = function (measurement) {
    this.count += 1;
    this.lastWorkData = this.workData;
    this.workData = this.workData + (measurement - this.workData) / this.count;
    this.S = this.S + (measurement - this.lastWorkData) * (measurement - this.workData);
};

// Performs the final step needed to get the standard deviation and returns it.
StandardDeviation.prototype.get = function () {
    if (this.count == 0) {
        throw new Error('Empty');
    }
    return Math.sqrt(this.S / (this.count));
};

// Replaces the value x currently present in this sample with the
// new value y. In a sliding window, x is the value that
// drops out and y is the new value entering the window. The sample
// count remains constant with this operation.
StandardDeviation.prototype.replace = function (x, y) {
    if (this.count == 0) {
        throw new Error('Empty');
    }
    const deltaYX = y - x;
    const deltaX = x - this.workData;
    const deltaY = y - this.workData;
    this.workData = this.workData + deltaYX / this.count;
    const deltaYp = y - this.workData;
    const countMinus1 = this.count - 1;
    this.S = this.S - this.count / countMinus1 * (deltaX * deltaX - deltaY * deltaYp) - deltaYX * deltaYp / countMinus1;
};

// Remove a measurement. Also calculates updates to stepwise parameters which are later used
// to determine sigma.
StandardDeviation.prototype.removeMeasurement = function (x) {
    if (this.count == 0) {
        throw new Error('Empty');
    } else if (this.count == 1) {
        this.workData = null;
        this.lastWorkData = null;
        this.S = 0;
        this.count = 1;
    }
    this.lastWorkData = (this.count * this.workData - x) / (this.count - 1);
    this.S -= (x - this.workData) * (x - this.lastWorkData);
    this.workData = this.lastWorkData;
    this.count -= 1;
};

module.exports = StandardDeviation;
