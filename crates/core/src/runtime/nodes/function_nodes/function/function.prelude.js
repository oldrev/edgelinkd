// Prelude script for every `function` node
const RED = (function () {
    return {
        util: {

            __cloneDeep: function (value, map = new WeakMap()) {
                if (value === null || typeof value !== 'object') {
                    return value;
                }

                if (map.has(value)) {
                    return map.get(value);
                }

                if (value instanceof Date) {
                    return new Date(value.getTime());
                }

                if (value instanceof RegExp) {
                    return new RegExp(value);
                }

                if (Array.isArray(value)) {
                    const clonedArray = value.map(item => this.__cloneDeep(item, map));
                    map.set(value, clonedArray);
                    return clonedArray;
                }

                const clonedObj = {};
                map.set(value, clonedObj);

                for (const key in value) {
                    if (value.hasOwnProperty(key)) {
                        clonedObj[key] = this.__cloneDeep(value[key], map);
                    }
                }

                return clonedObj;
            },

            cloneMessage: function (msg) {
                // FROM node-red
                if (typeof msg !== "undefined" && msg !== null) {
                    // Temporary fix for #97
                    // TODO: remove this http-node-specific fix somehow
                    var req = msg.req;
                    var res = msg.res;
                    delete msg.req;
                    delete msg.res;
                    var m = this.__cloneDeep(msg);
                    if (req) {
                        m.req = req;
                        msg.req = req;
                    }
                    if (res) {
                        m.res = res;
                        msg.res = res;
                    }
                    return m;
                }
                return msg;

            }
        }
    };
})();
