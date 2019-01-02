module.exports = {
    pad: (string, size) => {
        let s = String(string);
        while (s.length < (size || 2)) { s = '0' + s; }
        return s;
    }
};