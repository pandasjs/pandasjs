var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// src/index.js
var index_exports = {};
__export(index_exports, {
  default: () => index_default
});
module.exports = __toCommonJS(index_exports);

// src/window.js
function isValid(v) {
  return v !== null && v !== void 0 && !Number.isNaN(v);
}
function windowAgg(values, windowSize, minPeriods, fn) {
  const mp = minPeriods === void 0 ? windowSize : minPeriods;
  const result = new Array(values.length);
  for (let i = 0; i < values.length; i++) {
    const start = Math.max(0, i - windowSize + 1);
    const win = [];
    for (let j = start; j <= i; j++) {
      if (isValid(values[j])) win.push(values[j]);
    }
    result[i] = win.length >= mp ? fn(win) : NaN;
  }
  return result;
}
function expandingAgg(values, minPeriods, fn) {
  const mp = minPeriods === void 0 ? 1 : minPeriods;
  const result = new Array(values.length);
  const acc = [];
  for (let i = 0; i < values.length; i++) {
    if (isValid(values[i])) acc.push(values[i]);
    result[i] = acc.length >= mp ? fn([...acc]) : NaN;
  }
  return result;
}
function aggSum(arr) {
  return arr.reduce((a, b) => a + b, 0);
}
function aggMean(arr) {
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}
function aggMin(arr) {
  return Math.min(...arr);
}
function aggMax(arr) {
  return Math.max(...arr);
}
function aggCount(arr) {
  return arr.length;
}
function aggStd(arr) {
  if (arr.length < 2) return NaN;
  const m = aggMean(arr);
  const s2 = arr.reduce((a, v) => a + (v - m) * (v - m), 0);
  return Math.sqrt(s2 / (arr.length - 1));
}
function aggVar(arr) {
  if (arr.length < 2) return NaN;
  const m = aggMean(arr);
  return arr.reduce((a, v) => a + (v - m) * (v - m), 0) / (arr.length - 1);
}
function aggMedian(arr) {
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 0) return (sorted[mid - 1] + sorted[mid]) / 2;
  return sorted[mid];
}
function Rolling(values, options) {
  const { window: windowSize, minPeriods, name, index } = options;
  const mp = minPeriods;
  function make(fn) {
    const result = windowAgg(values, windowSize, mp, fn);
    return Series(result, { name, index: [...index], dtype: "float64" });
  }
  function aggSem(arr) {
    if (arr.length < 2) return NaN;
    return aggStd(arr) / Math.sqrt(arr.length);
  }
  function aggSkew(arr) {
    const n = arr.length;
    if (n < 3) return NaN;
    const m = aggMean(arr);
    let m2 = 0, m3 = 0;
    for (const v of arr) {
      const d = v - m;
      m2 += d * d;
      m3 += d * d * d;
    }
    const sd = Math.sqrt(m2 / (n - 1));
    return n / ((n - 1) * (n - 2)) * (m3 / (sd * sd * sd));
  }
  function aggKurt(arr) {
    const n = arr.length;
    if (n < 4) return NaN;
    const m = aggMean(arr);
    let m2 = 0, m4 = 0;
    for (const v of arr) {
      const d = v - m;
      m2 += d * d;
      m4 += d * d * d * d;
    }
    const variance = m2 / (n - 1);
    const num = n * (n + 1) / ((n - 1) * (n - 2) * (n - 3)) * (m4 / (variance * variance));
    return num - 3 * (n - 1) * (n - 1) / ((n - 2) * (n - 3));
  }
  function aggQuantile(q) {
    return function(arr) {
      const sorted = [...arr].sort((a, b) => a - b);
      const pos = (sorted.length - 1) * q;
      const lo = Math.floor(pos), hi = Math.ceil(pos);
      if (lo === hi) return sorted[lo];
      return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
    };
  }
  return {
    sum() {
      return make(aggSum);
    },
    mean() {
      return make(aggMean);
    },
    min() {
      return make(aggMin);
    },
    max() {
      return make(aggMax);
    },
    std() {
      return make(aggStd);
    },
    var() {
      return make(aggVar);
    },
    count() {
      return make(aggCount);
    },
    median() {
      return make(aggMedian);
    },
    apply(fn) {
      return make(fn);
    },
    quantile(q) {
      return make(aggQuantile(q));
    },
    sem() {
      return make(aggSem);
    },
    skew() {
      return make(aggSkew);
    },
    kurt() {
      return make(aggKurt);
    }
  };
}
function Expanding(values, options) {
  const { minPeriods, name, index } = options;
  const mp = minPeriods;
  function make(fn) {
    const result = expandingAgg(values, mp, fn);
    return Series(result, { name, index: [...index], dtype: "float64" });
  }
  return {
    sum() {
      return make(aggSum);
    },
    mean() {
      return make(aggMean);
    },
    min() {
      return make(aggMin);
    },
    max() {
      return make(aggMax);
    },
    std() {
      return make(aggStd);
    },
    var() {
      return make(aggVar);
    },
    count() {
      return make(aggCount);
    },
    apply(fn) {
      return make(fn);
    },
    quantile(q) {
      const qfn = function(arr) {
        const sorted = [...arr].sort((a, b) => a - b);
        const pos = (sorted.length - 1) * q;
        const lo = Math.floor(pos), hi = Math.ceil(pos);
        if (lo === hi) return sorted[lo];
        return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo);
      };
      return make(qfn);
    },
    sem() {
      const semFn = function(arr) {
        if (arr.length < 2) return NaN;
        return aggStd(arr) / Math.sqrt(arr.length);
      };
      return make(semFn);
    }
  };
}
function Ewm(values, options) {
  const { span, name, index } = options;
  const alpha = 2 / (span + 1);
  return {
    mean() {
      const result = new Array(values.length);
      const validVals = [];
      for (let i = 0; i < values.length; i++) {
        if (isValid(values[i])) {
          validVals.push(values[i]);
          let num = 0, den = 0;
          for (let j = 0; j < validVals.length; j++) {
            const w = Math.pow(1 - alpha, validVals.length - 1 - j);
            num += w * validVals[j];
            den += w;
          }
          result[i] = num / den;
        } else {
          result[i] = NaN;
        }
      }
      return Series(result, { name, index: [...index], dtype: "float64" });
    }
  };
}

// src/datetime.js
function toDatetime(data) {
  const values = data._isPandasSeries ? data.values : Array.isArray(data) ? data : [data];
  const dates = values.map((v) => {
    if (v === null || v === void 0) return null;
    if (v instanceof Date) return v;
    return new Date(v);
  });
  const name = data._isPandasSeries ? data.name : null;
  const index = data._isPandasSeries ? [...data.index] : dates.map((_, i) => i);
  return Series(dates, { name, index, dtype: "datetime64" });
}
function dateRange(options) {
  const { start, periods, freq = "D", end } = options;
  const dates = [];
  const startDate = new Date(start);
  const count = periods || 0;
  for (let i = 0; i < count; i++) {
    const d = new Date(startDate);
    if (freq === "D") {
      d.setDate(d.getDate() + i);
    } else if (freq === "H") {
      d.setHours(d.getHours() + i);
    } else if (freq === "M") {
      d.setMonth(d.getMonth() + i);
    } else if (freq === "Y") {
      d.setFullYear(d.getFullYear() + i);
    }
    dates.push(d);
  }
  return dates;
}
function DtAccessor(values, options) {
  const { name, index } = options;
  function mapDates(fn) {
    return Series(values.map((v) => v === null ? null : fn(v)), { name, index: [...index] });
  }
  return {
    get year() {
      return mapDates((d) => d.getFullYear());
    },
    get month() {
      return mapDates((d) => d.getMonth() + 1);
    },
    get day() {
      return mapDates((d) => d.getDate());
    },
    get hour() {
      return mapDates((d) => d.getHours());
    },
    get minute() {
      return mapDates((d) => d.getMinutes());
    },
    get second() {
      return mapDates((d) => d.getSeconds());
    },
    get dayofweek() {
      return mapDates((d) => (d.getDay() + 6) % 7);
    },
    get dayofyear() {
      return mapDates((d) => {
        const jan1 = new Date(d.getFullYear(), 0, 1);
        return Math.floor((d - jan1) / 864e5) + 1;
      });
    },
    get quarter() {
      return mapDates((d) => Math.floor(d.getMonth() / 3) + 1);
    },
    strftime(fmt) {
      return mapDates((d) => {
        let result = fmt;
        result = result.replace("%Y", String(d.getFullYear()));
        result = result.replace("%m", String(d.getMonth() + 1).padStart(2, "0"));
        result = result.replace("%d", String(d.getDate()).padStart(2, "0"));
        result = result.replace("%H", String(d.getHours()).padStart(2, "0"));
        result = result.replace("%M", String(d.getMinutes()).padStart(2, "0"));
        result = result.replace("%S", String(d.getSeconds()).padStart(2, "0"));
        return result;
      });
    }
  };
}
var to_datetime = toDatetime;
var date_range = dateRange;

// src/str.js
function StrAccessor(values, options) {
  const { name, index } = options;
  function mapStr(fn) {
    return Series(values.map((v) => v === null || v === void 0 ? null : fn(v)), { name, index: [...index] });
  }
  function mapBool(fn) {
    return values.map((v) => v === null || v === void 0 ? false : fn(v));
  }
  return {
    upper() {
      return mapStr((v) => v.toUpperCase());
    },
    lower() {
      return mapStr((v) => v.toLowerCase());
    },
    len() {
      return mapStr((v) => v.length);
    },
    strip() {
      return mapStr((v) => v.trim());
    },
    lstrip() {
      return mapStr((v) => v.trimStart());
    },
    rstrip() {
      return mapStr((v) => v.trimEnd());
    },
    contains(pat) {
      return mapBool((v) => v.includes(pat));
    },
    startswith(pat) {
      return mapBool((v) => v.startsWith(pat));
    },
    endswith(pat) {
      return mapBool((v) => v.endsWith(pat));
    },
    replace(pat, repl) {
      return mapStr((v) => v.split(pat).join(repl));
    },
    slice(start, stop) {
      return mapStr((v) => v.slice(start, stop));
    },
    title() {
      return mapStr((v) => v.replace(/\w\S*/g, (t) => t.charAt(0).toUpperCase() + t.substr(1).toLowerCase()));
    },
    capitalize() {
      return mapStr((v) => v.charAt(0).toUpperCase() + v.slice(1).toLowerCase());
    },
    swapcase() {
      return mapStr((v) => v.split("").map((c) => c === c.toUpperCase() ? c.toLowerCase() : c.toUpperCase()).join(""));
    },
    split(sep) {
      return Series(values.map((v) => v === null || v === void 0 ? null : v.split(sep)), { name, index: [...index] });
    },
    get(i) {
      return Series(values.map((v) => {
        if (v === null || v === void 0) return null;
        if (Array.isArray(v)) return v[i] !== void 0 ? v[i] : null;
        return v[i] !== void 0 ? v[i] : null;
      }), { name, index: [...index] });
    },
    find(sub) {
      return mapStr((v) => v.indexOf(sub));
    },
    count(pat) {
      return mapStr((v) => {
        let count = 0, pos = 0;
        while ((pos = v.indexOf(pat, pos)) !== -1) {
          count++;
          pos += pat.length;
        }
        return count;
      });
    },
    pad(width, options2) {
      const opts = options2 || {};
      const side = opts.side || "left";
      const fillchar = opts.fillchar || " ";
      return mapStr((v) => {
        if (side === "left") return v.padStart(width, fillchar);
        if (side === "right") return v.padEnd(width, fillchar);
        const total = width - v.length;
        if (total <= 0) return v;
        const left = Math.floor(total / 2);
        const right = total - left;
        return fillchar.repeat(left) + v + fillchar.repeat(right);
      });
    },
    zfill(width) {
      return mapStr((v) => v.padStart(width, "0"));
    },
    isalpha() {
      return mapBool((v) => v.length > 0 && /^[a-zA-Z]+$/.test(v));
    },
    isdigit() {
      return mapBool((v) => v.length > 0 && /^\d+$/.test(v));
    },
    isnumeric() {
      return mapBool((v) => v.length > 0 && /^\d+$/.test(v));
    },
    cat(options2) {
      const opts = options2 || {};
      const sep = opts.sep || "";
      return values.filter((v) => v !== null && v !== void 0).join(sep);
    },
    extract(pattern) {
      const re = new RegExp(pattern);
      return mapStr((v) => {
        const m = v.match(re);
        if (m && m[1]) return m[1];
        return null;
      });
    },
    center(width, fillchar) {
      const fc = fillchar || " ";
      return mapStr((v) => {
        const total = width - v.length;
        if (total <= 0) return v;
        const left = Math.floor(total / 2);
        const right = total - left;
        return fc.repeat(left) + v + fc.repeat(right);
      });
    },
    ljust(width, fillchar) {
      const fc = fillchar || " ";
      return mapStr((v) => v.padEnd(width, fc));
    },
    rjust(width, fillchar) {
      const fc = fillchar || " ";
      return mapStr((v) => v.padStart(width, fc));
    },
    rfind(sub) {
      return mapStr((v) => v.lastIndexOf(sub));
    },
    match(pattern) {
      const re = new RegExp("^" + pattern);
      return mapBool((v) => re.test(v));
    },
    fullmatch(pattern) {
      const re = new RegExp("^" + pattern + "$");
      return mapBool((v) => re.test(v));
    },
    isalnum() {
      return mapBool((v) => v.length > 0 && /^[a-zA-Z0-9]+$/.test(v));
    },
    isspace() {
      return mapBool((v) => v.length > 0 && /^\s+$/.test(v));
    },
    islower() {
      return mapBool((v) => v.length > 0 && v === v.toLowerCase() && v !== v.toUpperCase());
    },
    isupper() {
      return mapBool((v) => v.length > 0 && v === v.toUpperCase() && v !== v.toLowerCase());
    },
    istitle() {
      return mapBool((v) => v.length > 0 && v === v.replace(/\w\S*/g, (t) => t.charAt(0).toUpperCase() + t.substr(1).toLowerCase()));
    },
    removeprefix(prefix) {
      return mapStr((v) => v.startsWith(prefix) ? v.slice(prefix.length) : v);
    },
    removesuffix(suffix) {
      return mapStr((v) => v.endsWith(suffix) ? v.slice(0, -suffix.length) : v);
    },
    rsplit(sep) {
      return Series(values.map((v) => v === null || v === void 0 ? null : v.split(sep)), { name, index: [...index] });
    },
    join(sep) {
      return mapStr((v) => Array.isArray(v) ? v.join(sep) : v);
    },
    repeat(n) {
      return mapStr((v) => v.repeat(n));
    }
  };
}

// src/series.js
var _DataFrame = null;
function setDataFrame(DF) {
  _DataFrame = DF;
}
function inferDtype(values) {
  if (values.length === 0) return "object";
  const first = values.find((v) => v !== null && v !== void 0);
  if (first === void 0) return "object";
  if (typeof first === "number") {
    if (values.every((v) => v === null || v === void 0 || Number.isInteger(v))) return "int64";
    return "float64";
  }
  if (typeof first === "boolean") return "bool";
  if (typeof first === "string") return "object";
  return "object";
}
function Series(data, options) {
  if (data instanceof Object && data._isPandasSeries) return data;
  const opts = options || {};
  const values = Array.isArray(data) ? [...data] : [];
  const name = opts.name || null;
  const index = opts.index ? [...opts.index] : values.map((_, i) => i);
  const dtype = opts.dtype || inferDtype(values);
  const series = {
    _isPandasSeries: true,
    values,
    name,
    index,
    dtype,
    get shape() {
      return [values.length];
    },
    get size() {
      return values.length;
    },
    head(n) {
      const count = n === void 0 ? 5 : n;
      return Series(values.slice(0, count), { name, index: index.slice(0, count), dtype });
    },
    tail(n) {
      const count = n === void 0 ? 5 : n;
      return Series(values.slice(-count), { name, index: index.slice(-count), dtype });
    },
    tolist() {
      return [...values];
    },
    toString() {
      const lines = [];
      const maxIdx = index.reduce((a, b) => String(a).length > String(b).length ? a : b, "");
      const pad = String(maxIdx).length;
      for (let i = 0; i < values.length; i++) {
        lines.push(String(index[i]).padStart(pad) + "    " + String(values[i]));
      }
      if (name) lines.push(`Name: ${name}, `);
      lines.push(`dtype: ${dtype}`);
      return lines.join("\n");
    },
    // integer-location based indexing
    iloc(arg) {
      if (typeof arg === "number") {
        const idx = arg < 0 ? values.length + arg : arg;
        return values[idx];
      }
      if (Array.isArray(arg)) {
        const sel = arg.map((i) => i < 0 ? values.length + i : i);
        return Series(sel.map((i) => values[i]), { name, index: sel.map((i) => index[i]), dtype });
      }
      if (typeof arg === "object" && arg !== null) {
        const { start = 0, stop = values.length, step = 1 } = arg;
        const s = start < 0 ? values.length + start : start;
        const e = stop < 0 ? values.length + stop : stop;
        const vals = [];
        const idxs = [];
        for (let i = s; step > 0 ? i < e : i > e; i += step) {
          vals.push(values[i]);
          idxs.push(index[i]);
        }
        return Series(vals, { name, index: idxs, dtype });
      }
      return void 0;
    },
    // label-based indexing
    loc(arg) {
      if (!Array.isArray(arg) && typeof arg !== "object") {
        const pos = index.indexOf(arg);
        if (pos === -1) return void 0;
        return values[pos];
      }
      if (Array.isArray(arg)) {
        if (arg.length === values.length && typeof arg[0] === "boolean") {
          const vals = [];
          const idxs = [];
          for (let i = 0; i < arg.length; i++) {
            if (arg[i]) {
              vals.push(values[i]);
              idxs.push(index[i]);
            }
          }
          return Series(vals, { name, index: idxs, dtype });
        }
        const positions = arg.map((l) => index.indexOf(l));
        return Series(positions.map((p) => values[p]), { name, index: arg, dtype });
      }
      if (typeof arg === "object" && arg !== null) {
        const s = arg.start !== void 0 ? index.indexOf(arg.start) : 0;
        const e = arg.stop !== void 0 ? index.indexOf(arg.stop) : values.length - 1;
        return Series(values.slice(s, e + 1), { name, index: index.slice(s, e + 1), dtype });
      }
      return void 0;
    },
    // single value by integer position
    iat(i) {
      const idx = i < 0 ? values.length + i : i;
      return values[idx];
    },
    // single value by label
    at(label) {
      const pos = index.indexOf(label);
      if (pos === -1) return void 0;
      return values[pos];
    },
    // --- Computation & Aggregation ---
    sum() {
      let s = 0;
      for (const v of values) if (v !== null && v !== void 0) s += v;
      return s;
    },
    mean() {
      let s = 0, c = 0;
      for (const v of values) if (v !== null && v !== void 0) {
        s += v;
        c++;
      }
      return c > 0 ? s / c : NaN;
    },
    median() {
      const nums = values.filter((v) => v !== null && v !== void 0).sort((a, b) => a - b);
      if (nums.length === 0) return NaN;
      const mid = Math.floor(nums.length / 2);
      if (nums.length % 2 === 0) return (nums[mid - 1] + nums[mid]) / 2;
      return nums[mid];
    },
    min() {
      let m = Infinity;
      for (const v of values) if (v !== null && v !== void 0 && v < m) m = v;
      return m === Infinity ? NaN : m;
    },
    max() {
      let m = -Infinity;
      for (const v of values) if (v !== null && v !== void 0 && v > m) m = v;
      return m === -Infinity ? NaN : m;
    },
    std(ddof) {
      const d = ddof === void 0 ? 1 : ddof;
      let s = 0, s2 = 0, c = 0;
      for (const v of values) {
        if (v !== null && v !== void 0) {
          s += v;
          s2 += v * v;
          c++;
        }
      }
      if (c <= d) return NaN;
      const mean = s / c;
      return Math.sqrt((s2 - c * mean * mean) / (c - d));
    },
    var(ddof) {
      const d = ddof === void 0 ? 1 : ddof;
      let s = 0, s2 = 0, c = 0;
      for (const v of values) {
        if (v !== null && v !== void 0) {
          s += v;
          s2 += v * v;
          c++;
        }
      }
      if (c <= d) return NaN;
      const mean = s / c;
      return (s2 - c * mean * mean) / (c - d);
    },
    count() {
      let c = 0;
      for (const v of values) if (v !== null && v !== void 0) c++;
      return c;
    },
    prod() {
      let p = 1;
      for (const v of values) if (v !== null && v !== void 0) p *= v;
      return p;
    },
    quantile(q) {
      const nums = values.filter((v) => v !== null && v !== void 0).sort((a, b) => a - b);
      if (nums.length === 0) return NaN;
      const pos = (nums.length - 1) * q;
      const lo = Math.floor(pos);
      const hi = Math.ceil(pos);
      if (lo === hi) return nums[lo];
      return nums[lo] + (nums[hi] - nums[lo]) * (pos - lo);
    },
    mode() {
      const counts = {};
      for (const v of values) {
        if (v !== null && v !== void 0) counts[v] = (counts[v] || 0) + 1;
      }
      let maxCount = 0;
      for (const k in counts) if (counts[k] > maxCount) maxCount = counts[k];
      const modes = [];
      for (const k in counts) {
        if (counts[k] === maxCount) modes.push(Number(k));
      }
      modes.sort((a, b) => a - b);
      return Series(modes, { name, dtype });
    },
    skew() {
      let s = 0, c = 0;
      for (const v of values) if (v !== null && v !== void 0) {
        s += v;
        c++;
      }
      if (c < 3) return NaN;
      const m = s / c;
      let m2 = 0, m3 = 0;
      for (const v of values) {
        if (v !== null && v !== void 0) {
          const d = v - m;
          m2 += d * d;
          m3 += d * d * d;
        }
      }
      const variance = m2 / (c - 1);
      const sd = Math.sqrt(variance);
      return c / ((c - 1) * (c - 2)) * (m3 / (sd * sd * sd));
    },
    kurt() {
      let s = 0, c = 0;
      for (const v of values) if (v !== null && v !== void 0) {
        s += v;
        c++;
      }
      if (c < 4) return NaN;
      const m = s / c;
      let m2 = 0, m4 = 0;
      for (const v of values) {
        if (v !== null && v !== void 0) {
          const d = v - m;
          m2 += d * d;
          m4 += d * d * d * d;
        }
      }
      const variance = m2 / (c - 1);
      const num = c * (c + 1) / ((c - 1) * (c - 2) * (c - 3)) * (m4 / (variance * variance));
      const correction = 3 * (c - 1) * (c - 1) / ((c - 2) * (c - 3));
      return num - correction;
    },
    abs() {
      return Series(values.map((v) => v !== null && v !== void 0 ? Math.abs(v) : v), { name, index: [...index], dtype });
    },
    cumsum() {
      let s = 0;
      const result = values.map((v) => {
        if (v !== null && v !== void 0) {
          s += v;
          return s;
        }
        return NaN;
      });
      return Series(result, { name, index: [...index], dtype: "float64" });
    },
    cumprod() {
      let p = 1;
      const result = values.map((v) => {
        if (v !== null && v !== void 0) {
          p *= v;
          return p;
        }
        return NaN;
      });
      return Series(result, { name, index: [...index], dtype: "float64" });
    },
    cummax() {
      let m = -Infinity;
      const result = values.map((v) => {
        if (v !== null && v !== void 0) {
          m = Math.max(m, v);
          return m;
        }
        return NaN;
      });
      return Series(result, { name, index: [...index], dtype: "float64" });
    },
    cummin() {
      let m = Infinity;
      const result = values.map((v) => {
        if (v !== null && v !== void 0) {
          m = Math.min(m, v);
          return m;
        }
        return NaN;
      });
      return Series(result, { name, index: [...index], dtype: "float64" });
    },
    diff(periods) {
      const p = periods === void 0 ? 1 : periods;
      const result = values.map((v, i) => {
        if (i < p) return NaN;
        const prev = values[i - p];
        if (v === null || v === void 0 || prev === null || prev === void 0) return NaN;
        return v - prev;
      });
      return Series(result, { name, index: [...index], dtype: "float64" });
    },
    pctChange(periods) {
      const p = periods === void 0 ? 1 : periods;
      const result = values.map((v, i) => {
        if (i < p) return NaN;
        const prev = values[i - p];
        if (v === null || v === void 0 || prev === null || prev === void 0 || prev === 0) return NaN;
        return (v - prev) / prev;
      });
      return Series(result, { name, index: [...index], dtype: "float64" });
    },
    round(decimals) {
      const d = decimals === void 0 ? 0 : decimals;
      const factor = Math.pow(10, d);
      return Series(values.map((v) => {
        if (v === null || v === void 0) return v;
        return Math.round(v * factor) / factor;
      }), { name, index: [...index], dtype });
    },
    clip(options2) {
      const { lower, upper } = options2;
      return Series(values.map((v) => {
        if (v === null || v === void 0) return v;
        let r = v;
        if (lower !== void 0 && r < lower) r = lower;
        if (upper !== void 0 && r > upper) r = upper;
        return r;
      }), { name, index: [...index], dtype });
    },
    unique() {
      return [...new Set(values)];
    },
    nunique() {
      return new Set(values).size;
    },
    valueCounts() {
      const counts = {};
      const firstSeen = {};
      for (let i = 0; i < values.length; i++) {
        const key = String(values[i]);
        counts[key] = (counts[key] || 0) + 1;
        if (firstSeen[key] === void 0) firstSeen[key] = i;
      }
      const entries = Object.entries(counts).sort((a, b) => {
        if (b[1] !== a[1]) return b[1] - a[1];
        return firstSeen[a[0]] - firstSeen[b[0]];
      });
      return Series(entries.map((e) => e[1]), { name, index: entries.map((e) => isNaN(Number(e[0])) ? e[0] : Number(e[0])) });
    },
    idxmin() {
      let minVal = Infinity, minIdx = null;
      for (let i = 0; i < values.length; i++) {
        if (values[i] !== null && values[i] !== void 0 && values[i] < minVal) {
          minVal = values[i];
          minIdx = index[i];
        }
      }
      return minIdx;
    },
    idxmax() {
      let maxVal = -Infinity, maxIdx = null;
      for (let i = 0; i < values.length; i++) {
        if (values[i] !== null && values[i] !== void 0 && values[i] > maxVal) {
          maxVal = values[i];
          maxIdx = index[i];
        }
      }
      return maxIdx;
    },
    // element-wise arithmetic
    add(other) {
      const ov = other._isPandasSeries ? other.values : Array.isArray(other) ? other : values.map(() => other);
      return Series(values.map((v, i) => v + ov[i]), { name, index: [...index] });
    },
    sub(other) {
      const ov = other._isPandasSeries ? other.values : Array.isArray(other) ? other : values.map(() => other);
      return Series(values.map((v, i) => v - ov[i]), { name, index: [...index] });
    },
    mul(other) {
      const ov = other._isPandasSeries ? other.values : Array.isArray(other) ? other : values.map(() => other);
      return Series(values.map((v, i) => v * ov[i]), { name, index: [...index] });
    },
    div(other) {
      const ov = other._isPandasSeries ? other.values : Array.isArray(other) ? other : values.map(() => other);
      return Series(values.map((v, i) => v / ov[i]), { name, index: [...index] });
    },
    // apply function to each element
    apply(fn) {
      return Series(values.map(fn), { name, index: [...index] });
    },
    // boolean mask filter
    filter(mask) {
      const vals = [];
      const idxs = [];
      for (let i = 0; i < mask.length; i++) {
        if (mask[i]) {
          vals.push(values[i]);
          idxs.push(index[i]);
        }
      }
      return Series(vals, { name, index: idxs, dtype });
    },
    // --- Reshaping ---
    sortValues(options2) {
      const ascending = !options2 || options2.ascending === void 0 ? true : options2.ascending;
      const paired = values.map((v, i) => ({ v, idx: index[i] }));
      paired.sort((a, b) => {
        if (a.v < b.v) return ascending ? -1 : 1;
        if (a.v > b.v) return ascending ? 1 : -1;
        return 0;
      });
      return Series(paired.map((p) => p.v), { name, index: paired.map((p) => p.idx), dtype });
    },
    sortIndex(options2) {
      const ascending = !options2 || options2.ascending === void 0 ? true : options2.ascending;
      const paired = index.map((idx, i) => ({ idx, v: values[i] }));
      paired.sort((a, b) => {
        if (a.idx < b.idx) return ascending ? -1 : 1;
        if (a.idx > b.idx) return ascending ? 1 : -1;
        return 0;
      });
      return Series(paired.map((p) => p.v), { name, index: paired.map((p) => p.idx), dtype });
    },
    drop(labels) {
      const dropSet = new Set(Array.isArray(labels) ? labels : [labels]);
      const vals = [];
      const idxs = [];
      for (let i = 0; i < values.length; i++) {
        if (!dropSet.has(index[i])) {
          vals.push(values[i]);
          idxs.push(index[i]);
        }
      }
      return Series(vals, { name, index: idxs, dtype });
    },
    rename(newName) {
      return Series([...values], { name: newName, index: [...index], dtype });
    },
    resetIndex(options2) {
      const drop = options2 && options2.drop;
      if (drop) {
        return Series([...values], { name, index: values.map((_, i) => i), dtype });
      }
      return _DataFrame({ index: [...index], [name || 0]: [...values] }, { columns: ["index", name || 0] });
    },
    dropna() {
      const vals = [];
      const idxs = [];
      for (let i = 0; i < values.length; i++) {
        if (values[i] !== null && values[i] !== void 0 && !Number.isNaN(values[i])) {
          vals.push(values[i]);
          idxs.push(index[i]);
        }
      }
      return Series(vals, { name, index: idxs, dtype });
    },
    fillna(value) {
      return Series(values.map((v) => v === null || v === void 0 || typeof v === "number" && Number.isNaN(v) ? value : v), { name, index: [...index] });
    },
    concat(other) {
      const ov = other._isPandasSeries ? other : Series(other);
      return Series([...values, ...ov.values], { name, index: [...index, ...ov.index] });
    },
    // map: apply function to each element
    map(fn) {
      return Series(values.map(fn), { name, index: [...index] });
    },
    // replace values
    replace(toReplace, value) {
      if (typeof toReplace === "object" && !Array.isArray(toReplace)) {
        return Series(values.map((v) => toReplace[v] !== void 0 ? toReplace[v] : v), { name, index: [...index] });
      }
      return Series(values.map((v) => v === toReplace ? value : v), { name, index: [...index] });
    },
    // isin
    isin(list) {
      const set = new Set(list);
      return values.map((v) => set.has(v));
    },
    // between (inclusive)
    between(left, right) {
      return values.map((v) => v >= left && v <= right);
    },
    // nlargest
    nlargest(n) {
      const paired = values.map((v, i) => ({ v, idx: index[i], pos: i }));
      paired.sort((a, b) => b.v - a.v || a.pos - b.pos);
      const top = paired.slice(0, n);
      return Series(top.map((p) => p.v), { name, index: top.map((p) => p.idx), dtype });
    },
    // nsmallest
    nsmallest(n) {
      const paired = values.map((v, i) => ({ v, idx: index[i], pos: i }));
      paired.sort((a, b) => a.v - b.v || a.pos - b.pos);
      const top = paired.slice(0, n);
      return Series(top.map((p) => p.v), { name, index: top.map((p) => p.idx), dtype });
    },
    // rank (average method)
    rank() {
      const sorted = values.map((v, i2) => ({ v, i: i2 })).sort((a, b) => a.v - b.v);
      const ranks = new Array(values.length);
      let i = 0;
      while (i < sorted.length) {
        let j = i;
        while (j < sorted.length && sorted[j].v === sorted[i].v) j++;
        const avgRank = (i + 1 + j) / 2;
        for (let k = i; k < j; k++) ranks[sorted[k].i] = avgRank;
        i = j;
      }
      return Series(ranks, { name, index: [...index], dtype: "float64" });
    },
    // shift
    shift(periods) {
      const p = periods === void 0 ? 1 : periods;
      const result = new Array(values.length).fill(NaN);
      for (let i = 0; i < values.length; i++) {
        const src = i - p;
        if (src >= 0 && src < values.length) {
          result[i] = values[src];
        }
      }
      return Series(result, { name, index: [...index], dtype: "float64" });
    },
    // where: keep values where cond is true, else NaN
    where(cond) {
      return Series(values.map((v, i) => cond[i] ? v : NaN), { name, index: [...index], dtype: "float64" });
    },
    // mask: set values to NaN where cond is true
    mask(cond) {
      return Series(values.map((v, i) => cond[i] ? NaN : v), { name, index: [...index], dtype: "float64" });
    },
    // duplicated
    duplicated(options2) {
      const keep = options2 && options2.keep || "first";
      const result = new Array(values.length).fill(false);
      if (keep === "first") {
        const seen = /* @__PURE__ */ new Set();
        for (let i = 0; i < values.length; i++) {
          if (seen.has(values[i])) {
            result[i] = true;
          } else {
            seen.add(values[i]);
          }
        }
      } else if (keep === "last") {
        const seen = /* @__PURE__ */ new Set();
        for (let i = values.length - 1; i >= 0; i--) {
          if (seen.has(values[i])) {
            result[i] = true;
          } else {
            seen.add(values[i]);
          }
        }
      }
      return result;
    },
    // drop_duplicates
    dropDuplicates(options2) {
      const keep = options2 && options2.keep || "first";
      const duped = series.duplicated({ keep });
      const vals = [];
      const idxs = [];
      for (let i = 0; i < values.length; i++) {
        if (!duped[i]) {
          vals.push(values[i]);
          idxs.push(index[i]);
        }
      }
      return Series(vals, { name, index: idxs, dtype });
    },
    // str accessor
    get str() {
      return StrAccessor(values, { name, index });
    },
    // dt accessor
    get dt() {
      return DtAccessor(values, { name, index });
    },
    explode() {
      const newVals = [];
      const newIdx = [];
      for (let i = 0; i < values.length; i++) {
        if (Array.isArray(values[i])) {
          for (const v of values[i]) {
            newVals.push(v);
            newIdx.push(index[i]);
          }
        } else {
          newVals.push(values[i]);
          newIdx.push(index[i]);
        }
      }
      return Series(newVals, { name, index: newIdx });
    },
    // comparison helpers returning boolean arrays
    gt(other) {
      return values.map((v) => v > other);
    },
    lt(other) {
      return values.map((v) => v < other);
    },
    ge(other) {
      return values.map((v) => v >= other);
    },
    le(other) {
      return values.map((v) => v <= other);
    },
    eq(other) {
      return values.map((v) => v === other);
    },
    ne(other) {
      return values.map((v) => v !== other);
    },
    astype(dtype2) {
      const castFns = {
        "int64": (v) => v === null || v === void 0 ? v : Math.trunc(Number(v)),
        "float64": (v) => v === null || v === void 0 ? v : Number(v),
        "string": (v) => v === null || v === void 0 ? v : String(v),
        "bool": (v) => v === null || v === void 0 ? v : Boolean(v)
      };
      const fn = castFns[dtype2];
      if (fn) return Series(values.map(fn), { name, index: [...index], dtype: dtype2 });
      return Series([...values], { name, index: [...index], dtype: dtype2 });
    },
    ffill() {
      const result = [...values];
      for (let i = 1; i < result.length; i++) {
        if (result[i] === null || result[i] === void 0 || typeof result[i] === "number" && Number.isNaN(result[i])) {
          result[i] = result[i - 1];
        }
      }
      return Series(result, { name, index: [...index], dtype });
    },
    bfill() {
      const result = [...values];
      for (let i = result.length - 2; i >= 0; i--) {
        if (result[i] === null || result[i] === void 0 || typeof result[i] === "number" && Number.isNaN(result[i])) {
          result[i] = result[i + 1];
        }
      }
      return Series(result, { name, index: [...index], dtype });
    },
    interpolate() {
      const result = [...values];
      for (let i = 0; i < result.length; i++) {
        if (result[i] === null || result[i] === void 0 || typeof result[i] === "number" && Number.isNaN(result[i])) {
          let prevIdx = -1;
          for (let j = i - 1; j >= 0; j--) {
            if (result[j] !== null && result[j] !== void 0 && !(typeof result[j] === "number" && Number.isNaN(result[j]))) {
              prevIdx = j;
              break;
            }
          }
          let nextIdx = -1;
          for (let j = i + 1; j < result.length; j++) {
            if (values[j] !== null && values[j] !== void 0 && !(typeof values[j] === "number" && Number.isNaN(values[j]))) {
              nextIdx = j;
              break;
            }
          }
          if (prevIdx >= 0 && nextIdx >= 0) {
            const ratio = (i - prevIdx) / (nextIdx - prevIdx);
            result[i] = result[prevIdx] + ratio * (values[nextIdx] - result[prevIdx]);
          } else if (prevIdx >= 0) {
            result[i] = result[prevIdx];
          }
        }
      }
      return Series(result, { name, index: [...index], dtype: "float64" });
    },
    copy() {
      return Series([...values], { name, index: [...index], dtype });
    },
    isna() {
      return values.map((v) => v === null || v === void 0 || typeof v === "number" && Number.isNaN(v));
    },
    notna() {
      return values.map((v) => v !== null && v !== void 0 && !(typeof v === "number" && Number.isNaN(v)));
    },
    pipe(fn) {
      return fn(series);
    },
    sample(options2) {
      const opts2 = options2 || {};
      const n = opts2.n || (opts2.frac ? Math.floor(values.length * opts2.frac) : 1);
      const seed = opts2.randomState;
      let rng = seed !== void 0 ? /* @__PURE__ */ (() => {
        let s = seed;
        return () => {
          s = s * 1103515245 + 12345 & 2147483647;
          return s / 2147483647;
        };
      })() : Math.random;
      const idxs = Array.from({ length: values.length }, (_, i) => i);
      for (let i = idxs.length - 1; i > 0; i--) {
        const j = Math.floor(rng() * (i + 1));
        const tmp = idxs[i];
        idxs[i] = idxs[j];
        idxs[j] = tmp;
      }
      const sel = idxs.slice(0, n);
      return Series(sel.map((i) => values[i]), { name, index: sel.map((i) => index[i]), dtype });
    },
    rolling(windowSize, options2) {
      const opts2 = options2 || {};
      return Rolling(values, { window: windowSize, minPeriods: opts2.minPeriods, name, index });
    },
    expanding(options2) {
      const opts2 = options2 || {};
      return Expanding(values, { minPeriods: opts2.minPeriods, name, index });
    },
    ewm(options2) {
      const { span } = options2;
      return Ewm(values, { span, name, index });
    },
    // floor division
    floordiv(other) {
      const ov = other._isPandasSeries ? other.values : Array.isArray(other) ? other : values.map(() => other);
      return Series(values.map((v, i) => Math.floor(v / ov[i])), { name, index: [...index] });
    },
    // modulo
    mod(other) {
      const ov = other._isPandasSeries ? other.values : Array.isArray(other) ? other : values.map(() => other);
      return Series(values.map((v, i) => v % ov[i]), { name, index: [...index] });
    },
    // power
    pow(other) {
      const ov = other._isPandasSeries ? other.values : Array.isArray(other) ? other : values.map(() => other);
      return Series(values.map((v, i) => Math.pow(v, ov[i])), { name, index: [...index] });
    },
    // true division (alias for div)
    truediv(other) {
      return series.div(other);
    },
    // reverse arithmetic
    radd(other) {
      const ov = other._isPandasSeries ? other.values : Array.isArray(other) ? other : values.map(() => other);
      return Series(values.map((v, i) => ov[i] + v), { name, index: [...index] });
    },
    rsub(other) {
      const ov = other._isPandasSeries ? other.values : Array.isArray(other) ? other : values.map(() => other);
      return Series(values.map((v, i) => ov[i] - v), { name, index: [...index] });
    },
    rmul(other) {
      const ov = other._isPandasSeries ? other.values : Array.isArray(other) ? other : values.map(() => other);
      return Series(values.map((v, i) => ov[i] * v), { name, index: [...index] });
    },
    rdiv(other) {
      const ov = other._isPandasSeries ? other.values : Array.isArray(other) ? other : values.map(() => other);
      return Series(values.map((v, i) => ov[i] / v), { name, index: [...index] });
    },
    rfloordiv(other) {
      const ov = other._isPandasSeries ? other.values : Array.isArray(other) ? other : values.map(() => other);
      return Series(values.map((v, i) => Math.floor(ov[i] / v)), { name, index: [...index] });
    },
    rmod(other) {
      const ov = other._isPandasSeries ? other.values : Array.isArray(other) ? other : values.map(() => other);
      return Series(values.map((v, i) => ov[i] % v), { name, index: [...index] });
    },
    rpow(other) {
      const ov = other._isPandasSeries ? other.values : Array.isArray(other) ? other : values.map(() => other);
      return Series(values.map((v, i) => Math.pow(ov[i], v)), { name, index: [...index] });
    },
    // fill nulls from other Series
    combineFirst(other) {
      const ov = other._isPandasSeries ? other : Series(other);
      const result = values.map((v, i) => {
        if (v === null || v === void 0 || typeof v === "number" && Number.isNaN(v)) {
          return ov.values[i];
        }
        return v;
      });
      return Series(result, { name, index: [...index] });
    },
    // show differences between two Series
    compare(other) {
      const ov = other._isPandasSeries ? other.values : other;
      const selfVals = [], otherVals = [], idxs = [];
      for (let i = 0; i < values.length; i++) {
        if (values[i] !== ov[i]) {
          selfVals.push(values[i]);
          otherVals.push(ov[i]);
          idxs.push(index[i]);
        }
      }
      return _DataFrame({ self: selfVals, other: otherVals }, { index: idxs });
    },
    // repeat values n times
    repeat(n) {
      const result = [], newIdx = [];
      for (let i = 0; i < values.length; i++) {
        for (let j = 0; j < n; j++) {
          result.push(values[i]);
          newIdx.push(index[i]);
        }
      }
      return Series(result, { name, index: newIdx, dtype });
    },
    // return indices that would sort
    argsort() {
      const paired = values.map((v, i) => ({ v, i }));
      paired.sort((a, b) => a.v - b.v);
      return Series(paired.map((p) => p.i), { name, index: [...index], dtype: "int64" });
    },
    // covariance with another Series
    cov(other) {
      const ov = other._isPandasSeries ? other.values : other;
      const n = values.length;
      if (n < 2) return NaN;
      let sx = 0, sy = 0;
      for (let i = 0; i < n; i++) {
        sx += values[i];
        sy += ov[i];
      }
      const mx = sx / n, my = sy / n;
      let sxy = 0;
      for (let i = 0; i < n; i++) sxy += (values[i] - mx) * (ov[i] - my);
      return sxy / (n - 1);
    },
    // autocorrelation at given lag
    autocorr(lag) {
      const p = lag === void 0 ? 1 : lag;
      const n = values.length - p;
      if (n < 2) return NaN;
      let sx = 0, sy = 0;
      for (let i = 0; i < n; i++) {
        sx += values[i];
        sy += values[i + p];
      }
      const mx = sx / n, my = sy / n;
      let sxy = 0, sx2 = 0, sy2 = 0;
      for (let i = 0; i < n; i++) {
        const dx = values[i] - mx, dy = values[i + p] - my;
        sxy += dx * dy;
        sx2 += dx * dx;
        sy2 += dy * dy;
      }
      return sxy / Math.sqrt(sx2 * sy2);
    },
    // standard error of the mean
    sem() {
      const c = series.count();
      if (c < 2) return NaN;
      return series.std() / Math.sqrt(c);
    },
    // convert to single-column DataFrame
    toFrame(colName) {
      const n = colName || name || 0;
      return _DataFrame({ [n]: [...values] }, { index: [...index] });
    },
    // convert to {index: value} dict
    toDict() {
      const obj = {};
      for (let i = 0; i < values.length; i++) {
        obj[String(index[i])] = values[i];
      }
      return obj;
    },
    // iterate as [index, value] pairs
    items() {
      return index.map((idx, i) => [idx, values[i]]);
    },
    // return index array
    keys() {
      return [...index];
    },
    toJSON() {
      const obj = {};
      for (let i = 0; i < values.length; i++) {
        obj[String(index[i])] = values[i];
      }
      return obj;
    }
  };
  series.sort_values = series.sortValues;
  series.sort_index = series.sortIndex;
  series.value_counts = series.valueCounts;
  series.drop_duplicates = series.dropDuplicates;
  series.reset_index = series.resetIndex;
  series.pct_change = series.pctChange;
  series.to_frame = series.toFrame;
  series.to_dict = series.toDict;
  series.to_json = series.toJSON;
  series.to_list = series.tolist;
  series.combine_first = series.combineFirst;
  series.transform = series.apply;
  series.agg = series.apply;
  series.aggregate = series.apply;
  series[Symbol.for("nodejs.util.inspect.custom")] = function() {
    return series.toString();
  };
  return series;
}

// src/groupby.js
function GroupBy(colData, columns, index, rowCount, by, DataFrame2) {
  const byArr = Array.isArray(by) ? by : [by];
  const groups = {};
  const groupOrder = [];
  for (let i = 0; i < rowCount; i++) {
    const key = byArr.map((b) => colData[b][i]).join("\0");
    if (groups[key] === void 0) {
      groups[key] = [];
      groupOrder.push(key);
    }
    groups[key].push(i);
  }
  const aggCols = columns.filter((c) => !byArr.includes(c));
  function makeIndex() {
    if (byArr.length === 1) {
      return groupOrder.map((k) => colData[byArr[0]][groups[k][0]]);
    }
    return groupOrder.map((k) => {
      return byArr.map((b) => colData[b][groups[k][0]]).join("\0");
    });
  }
  function aggWith(fn) {
    const numAggCols = aggCols.filter((c) => {
      const first = colData[c].find((v) => v !== null && v !== void 0);
      return typeof first === "number";
    });
    const result2 = {};
    for (const c of numAggCols) {
      result2[c] = groupOrder.map((k) => fn(groups[k].map((i) => colData[c][i])));
    }
    return DataFrame2(result2, { columns: numAggCols, index: makeIndex() });
  }
  function aggFnByName(name) {
    const fns = {
      sum: (arr) => arr.filter((v) => v !== null && v !== void 0).reduce((a, b) => a + b, 0),
      mean: (arr) => {
        const valid = arr.filter((v) => v !== null && v !== void 0);
        return valid.length > 0 ? valid.reduce((a, b) => a + b, 0) / valid.length : NaN;
      },
      count: (arr) => arr.filter((v) => v !== null && v !== void 0).length,
      min: (arr) => {
        const valid = arr.filter((v) => v !== null && v !== void 0);
        return valid.length > 0 ? Math.min(...valid) : NaN;
      },
      max: (arr) => {
        const valid = arr.filter((v) => v !== null && v !== void 0);
        return valid.length > 0 ? Math.max(...valid) : NaN;
      },
      std: (arr) => {
        const valid = arr.filter((v) => v !== null && v !== void 0);
        if (valid.length < 2) return NaN;
        const m = valid.reduce((a, b) => a + b, 0) / valid.length;
        const s2 = valid.reduce((a, v) => a + (v - m) * (v - m), 0);
        return Math.sqrt(s2 / (valid.length - 1));
      },
      median: (arr) => {
        const sorted = arr.filter((v) => v !== null && v !== void 0).sort((a, b) => a - b);
        if (sorted.length === 0) return NaN;
        const mid = Math.floor(sorted.length / 2);
        if (sorted.length % 2 === 0) return (sorted[mid - 1] + sorted[mid]) / 2;
        return sorted[mid];
      },
      first: (arr) => arr.length > 0 ? arr[0] : null,
      last: (arr) => arr.length > 0 ? arr[arr.length - 1] : null
    };
    return fns[name];
  }
  const result = {
    sum() {
      return aggWith(aggFnByName("sum"));
    },
    mean() {
      return aggWith(aggFnByName("mean"));
    },
    count() {
      return aggWith(aggFnByName("count"));
    },
    min() {
      return aggWith(aggFnByName("min"));
    },
    max() {
      return aggWith(aggFnByName("max"));
    },
    std() {
      return aggWith(aggFnByName("std"));
    },
    median() {
      return aggWith(aggFnByName("median"));
    },
    first() {
      return aggWith(aggFnByName("first"));
    },
    last() {
      return aggWith(aggFnByName("last"));
    },
    size() {
      const sizes = groupOrder.map((k) => groups[k].length);
      return Series(sizes, { index: makeIndex() });
    },
    agg(spec) {
      if (typeof spec === "string") {
        return aggWith(aggFnByName(spec));
      }
      const resultData = {};
      const resultCols = [];
      for (const [col, fns] of Object.entries(spec)) {
        if (typeof fns === "string") {
          const fn = aggFnByName(fns);
          resultData[col] = groupOrder.map((k) => fn(groups[k].map((i) => colData[col][i])));
          if (!resultCols.includes(col)) resultCols.push(col);
        } else if (Array.isArray(fns)) {
          for (const fnName of fns) {
            const fn = aggFnByName(fnName);
            const key = col + "_" + fnName;
            resultData[key] = groupOrder.map((k) => fn(groups[k].map((i) => colData[col][i])));
            resultCols.push(key);
          }
        }
      }
      return DataFrame2(resultData, { columns: resultCols, index: makeIndex() });
    },
    transform(fn) {
      const resultData = {};
      const numAggCols = aggCols.filter((c) => {
        const first = colData[c].find((v) => v !== null && v !== void 0);
        return typeof first === "number";
      });
      for (const c of numAggCols) {
        resultData[c] = new Array(rowCount);
      }
      for (const k of groupOrder) {
        const idxs = groups[k];
        for (const c of numAggCols) {
          const groupVals = idxs.map((i) => colData[c][i]);
          const transformed = fn(groupVals);
          if (typeof transformed === "number") {
            for (const i of idxs) resultData[c][i] = transformed;
          } else {
            for (let j = 0; j < idxs.length; j++) resultData[c][idxs[j]] = transformed[j];
          }
        }
      }
      return DataFrame2(resultData, { columns: numAggCols, index: [...index] });
    },
    filter(fn) {
      const keepIdxs = [];
      for (const k of groupOrder) {
        const idxs = groups[k];
        const groupData = {};
        for (const c of columns) {
          groupData[c] = idxs.map((i) => colData[c][i]);
        }
        const groupDf = DataFrame2(groupData, { columns: [...columns], index: idxs.map((i) => index[i]) });
        if (fn(groupDf)) {
          for (const i of idxs) keepIdxs.push(i);
        }
      }
      const sliced = {};
      for (const c of columns) sliced[c] = keepIdxs.map((i) => colData[c][i]);
      return DataFrame2(sliced, { columns: [...columns], index: keepIdxs.map((i) => index[i]) });
    },
    // cumulative operations within groups
    cumsum() {
      const resultData = {};
      for (const c of aggCols) resultData[c] = new Array(rowCount);
      for (const k of groupOrder) {
        for (const c of aggCols) {
          let s = 0;
          for (const i of groups[k]) {
            s += colData[c][i];
            resultData[c][i] = s;
          }
        }
      }
      return DataFrame2(resultData, { columns: aggCols, index: [...index] });
    },
    cumprod() {
      const resultData = {};
      for (const c of aggCols) resultData[c] = new Array(rowCount);
      for (const k of groupOrder) {
        for (const c of aggCols) {
          let p = 1;
          for (const i of groups[k]) {
            p *= colData[c][i];
            resultData[c][i] = p;
          }
        }
      }
      return DataFrame2(resultData, { columns: aggCols, index: [...index] });
    },
    cummin() {
      const resultData = {};
      for (const c of aggCols) resultData[c] = new Array(rowCount);
      for (const k of groupOrder) {
        for (const c of aggCols) {
          let m = Infinity;
          for (const i of groups[k]) {
            m = Math.min(m, colData[c][i]);
            resultData[c][i] = m;
          }
        }
      }
      return DataFrame2(resultData, { columns: aggCols, index: [...index] });
    },
    cummax() {
      const resultData = {};
      for (const c of aggCols) resultData[c] = new Array(rowCount);
      for (const k of groupOrder) {
        for (const c of aggCols) {
          let m = -Infinity;
          for (const i of groups[k]) {
            m = Math.max(m, colData[c][i]);
            resultData[c][i] = m;
          }
        }
      }
      return DataFrame2(resultData, { columns: aggCols, index: [...index] });
    },
    cumcount() {
      const result2 = new Array(rowCount);
      for (const k of groupOrder) {
        let c = 0;
        for (const i of groups[k]) {
          result2[i] = c;
          c++;
        }
      }
      return Series(result2, { index: [...index] });
    },
    nth(n) {
      const idxs = [];
      for (const k of groupOrder) {
        const g = groups[k];
        const pos = n < 0 ? g.length + n : n;
        if (pos >= 0 && pos < g.length) idxs.push(g[pos]);
      }
      const sliced = {};
      for (const c of columns) sliced[c] = idxs.map((i) => colData[c][i]);
      return DataFrame2(sliced, { columns: [...columns], index: idxs.map((i) => index[i]) });
    },
    head(n) {
      const count = n === void 0 ? 5 : n;
      const idxs = [];
      for (const k of groupOrder) {
        const g = groups[k];
        for (let i = 0; i < Math.min(count, g.length); i++) idxs.push(g[i]);
      }
      const sliced = {};
      for (const c of columns) sliced[c] = idxs.map((i) => colData[c][i]);
      return DataFrame2(sliced, { columns: [...columns], index: idxs.map((i) => index[i]) });
    },
    tail(n) {
      const count = n === void 0 ? 5 : n;
      const idxSet = /* @__PURE__ */ new Set();
      for (const k of groupOrder) {
        const g = groups[k];
        const start = Math.max(0, g.length - count);
        for (let i = start; i < g.length; i++) idxSet.add(g[i]);
      }
      const idxs = [];
      for (let i = 0; i < rowCount; i++) {
        if (idxSet.has(i)) idxs.push(i);
      }
      const sliced = {};
      for (const c of columns) sliced[c] = idxs.map((i) => colData[c][i]);
      return DataFrame2(sliced, { columns: [...columns], index: idxs.map((i) => index[i]) });
    },
    ngroup() {
      const result2 = new Array(rowCount);
      for (let g = 0; g < groupOrder.length; g++) {
        for (const i of groups[groupOrder[g]]) result2[i] = g;
      }
      return Series(result2, { index: [...index] });
    },
    rank() {
      const resultData = {};
      for (const c of aggCols) resultData[c] = new Array(rowCount);
      for (const k of groupOrder) {
        const idxs = groups[k];
        const sorted = idxs.map((i) => ({ v: colData[aggCols[0]][i], i }));
        for (const c of aggCols) {
          const grpSorted = idxs.map((i) => ({ v: colData[c][i], i })).sort((a, b) => a.v - b.v);
          let j = 0;
          while (j < grpSorted.length) {
            let end = j;
            while (end < grpSorted.length && grpSorted[end].v === grpSorted[j].v) end++;
            const avgRank = (j + 1 + end) / 2;
            for (let t = j; t < end; t++) resultData[c][grpSorted[t].i] = avgRank;
            j = end;
          }
        }
      }
      return DataFrame2(resultData, { columns: aggCols, index: [...index] });
    },
    getGroup(key) {
      const keyStr = Array.isArray(key) ? key.join("\0") : String(key);
      const idxs = groups[keyStr];
      if (!idxs) return null;
      const sliced = {};
      for (const c of columns) sliced[c] = idxs.map((i) => colData[c][i]);
      return DataFrame2(sliced, { columns: [...columns], index: idxs.map((i) => index[i]) });
    },
    describe() {
      const frames = [];
      for (const k of groupOrder) {
        const idxs = groups[k];
        const groupData = {};
        for (const c of columns) groupData[c] = idxs.map((i) => colData[c][i]);
        const groupDf = DataFrame2(groupData, { columns: [...columns], index: idxs.map((i) => index[i]) });
        frames.push(groupDf.describe());
      }
      return frames;
    },
    shift(periods) {
      const p = periods === void 0 ? 1 : periods;
      const resultData = {};
      for (const c of aggCols) resultData[c] = new Array(rowCount);
      for (const k of groupOrder) {
        const idxs = groups[k];
        for (let j = 0; j < idxs.length; j++) {
          const srcIdx = j - p;
          resultData[aggCols[0]];
          for (const c of aggCols) {
            resultData[c][idxs[j]] = srcIdx >= 0 && srcIdx < idxs.length ? colData[c][idxs[srcIdx]] : NaN;
          }
        }
      }
      return DataFrame2(resultData, { columns: aggCols, index: [...index] });
    },
    diff(periods) {
      const p = periods === void 0 ? 1 : periods;
      const resultData = {};
      for (const c of aggCols) resultData[c] = new Array(rowCount);
      for (const k of groupOrder) {
        const idxs = groups[k];
        for (let j = 0; j < idxs.length; j++) {
          for (const c of aggCols) {
            if (j < p) {
              resultData[c][idxs[j]] = NaN;
              continue;
            }
            resultData[c][idxs[j]] = colData[c][idxs[j]] - colData[c][idxs[j - p]];
          }
        }
      }
      return DataFrame2(resultData, { columns: aggCols, index: [...index] });
    },
    fillna(value) {
      const resultData = {};
      for (const c of columns) resultData[c] = [...colData[c]];
      for (const k of groupOrder) {
        for (const i of groups[k]) {
          for (const c of aggCols) {
            const v = resultData[c][i];
            if (v === null || v === void 0 || typeof v === "number" && Number.isNaN(v)) {
              resultData[c][i] = value;
            }
          }
        }
      }
      return DataFrame2(resultData, { columns: [...columns], index: [...index] });
    },
    ffill() {
      const resultData = {};
      for (const c of columns) resultData[c] = [...colData[c]];
      for (const k of groupOrder) {
        const idxs = groups[k];
        for (const c of aggCols) {
          for (let j = 1; j < idxs.length; j++) {
            const v = resultData[c][idxs[j]];
            if (v === null || v === void 0 || typeof v === "number" && Number.isNaN(v)) {
              resultData[c][idxs[j]] = resultData[c][idxs[j - 1]];
            }
          }
        }
      }
      return DataFrame2(resultData, { columns: [...columns], index: [...index] });
    },
    bfill() {
      const resultData = {};
      for (const c of columns) resultData[c] = [...colData[c]];
      for (const k of groupOrder) {
        const idxs = groups[k];
        for (const c of aggCols) {
          for (let j = idxs.length - 2; j >= 0; j--) {
            const v = resultData[c][idxs[j]];
            if (v === null || v === void 0 || typeof v === "number" && Number.isNaN(v)) {
              resultData[c][idxs[j]] = resultData[c][idxs[j + 1]];
            }
          }
        }
      }
      return DataFrame2(resultData, { columns: [...columns], index: [...index] });
    },
    pipe(fn) {
      return fn(result);
    },
    apply(fn) {
      const frames = [];
      for (const k of groupOrder) {
        const idxs = groups[k];
        const groupData = {};
        for (const c of columns) groupData[c] = idxs.map((i) => colData[c][i]);
        const groupDf = DataFrame2(groupData, { columns: [...columns], index: idxs.map((i) => index[i]) });
        frames.push(fn(groupDf));
      }
      if (frames.length === 0) return DataFrame2({}, { columns });
      if (frames[0]._isPandasDataFrame) {
        const allCols = frames[0].columns;
        const resultData = {};
        for (const c of allCols) resultData[c] = [];
        let allIndex = [];
        for (const f of frames) {
          allIndex = allIndex.concat(f.index);
          for (const c of allCols) {
            const cd = f._colData(c);
            for (const v of cd) resultData[c].push(v);
          }
        }
        return DataFrame2(resultData, { columns: allCols, index: allIndex });
      }
      if (frames[0]._isPandasSeries) {
        let vals = [], idxs = [];
        for (const f of frames) {
          vals = vals.concat(f.values);
          idxs = idxs.concat(f.index);
        }
        return Series(vals, { index: idxs });
      }
      return Series(frames, { index: makeIndex() });
    }
  };
  result.get_group = result.getGroup;
  return result;
}

// src/dataframe.js
function DataFrame(data, options) {
  if (data instanceof Object && data._isPandasDataFrame) return data;
  const opts = options || {};
  let columns = [];
  let colData = {};
  let rowCount = 0;
  if (data && !Array.isArray(data) && typeof data === "object") {
    columns = opts.columns || Object.keys(data);
    for (const col of columns) {
      colData[col] = Array.isArray(data[col]) ? [...data[col]] : [];
    }
    rowCount = columns.length > 0 ? colData[columns[0]].length : 0;
  } else if (Array.isArray(data) && data.length > 0 && typeof data[0] === "object") {
    const allKeys = /* @__PURE__ */ new Set();
    for (const row of data) {
      for (const k of Object.keys(row)) allKeys.add(k);
    }
    columns = opts.columns || [...allKeys];
    for (const col of columns) {
      colData[col] = data.map((row) => row[col] !== void 0 ? row[col] : null);
    }
    rowCount = data.length;
  } else if (!data || Array.isArray(data) && data.length === 0) {
    columns = opts.columns || [];
    for (const col of columns) {
      colData[col] = [];
    }
    rowCount = 0;
  }
  const index = opts.index ? [...opts.index] : Array.from({ length: rowCount }, (_, i) => i);
  const df = {
    _isPandasDataFrame: true,
    columns: [...columns],
    index,
    get shape() {
      return [rowCount, columns.length];
    },
    get dtypes() {
      const result = {};
      for (const col of columns) {
        result[col] = inferDtype(colData[col]);
      }
      return result;
    },
    get values() {
      const rows = [];
      for (let i = 0; i < rowCount; i++) {
        const row = [];
        for (const col of columns) {
          row.push(colData[col][i]);
        }
        rows.push(row);
      }
      return rows;
    },
    col(name) {
      if (colData[name] === void 0) return null;
      return Series(colData[name], { name, index: [...index] });
    },
    head(n) {
      const count = n === void 0 ? 5 : n;
      const sliced = {};
      for (const col of columns) {
        sliced[col] = colData[col].slice(0, count);
      }
      return DataFrame(sliced, { columns: [...columns], index: index.slice(0, count) });
    },
    tail(n) {
      const count = n === void 0 ? 5 : n;
      const sliced = {};
      for (const col of columns) {
        sliced[col] = colData[col].slice(-count);
      }
      return DataFrame(sliced, { columns: [...columns], index: index.slice(-count) });
    },
    toString() {
      const colWidths = {};
      for (const col of columns) {
        colWidths[col] = col.length;
        for (let i = 0; i < rowCount; i++) {
          colWidths[col] = Math.max(colWidths[col], String(colData[col][i]).length);
        }
      }
      const idxWidth = index.reduce((m, v) => Math.max(m, String(v).length), 0);
      const lines = [];
      let header = "".padStart(idxWidth) + "  ";
      header += columns.map((c) => c.padStart(colWidths[c])).join("  ");
      lines.push(header);
      for (let i = 0; i < rowCount; i++) {
        let line = String(index[i]).padStart(idxWidth) + "  ";
        line += columns.map((c) => String(colData[c][i]).padStart(colWidths[c])).join("  ");
        lines.push(line);
      }
      return lines.join("\n");
    },
    toJSON() {
      const rows = [];
      for (let i = 0; i < rowCount; i++) {
        const row = {};
        for (const col of columns) {
          row[col] = colData[col][i];
        }
        rows.push(row);
      }
      return rows;
    },
    // integer-location based indexing
    iloc(row, colArg) {
      if (colArg === void 0) {
        if (typeof row === "number") {
          const r = row < 0 ? rowCount + row : row;
          const obj = {};
          for (const c of columns) obj[c] = colData[c][r];
          return obj;
        }
        if (Array.isArray(row)) {
          const sliced = {};
          const idxs = row.map((i) => i < 0 ? rowCount + i : i);
          for (const c of columns) sliced[c] = idxs.map((i) => colData[c][i]);
          return DataFrame(sliced, { columns: [...columns], index: idxs.map((i) => index[i]) });
        }
        if (typeof row === "object" && row !== null) {
          const { start = 0, stop = rowCount, step = 1 } = row;
          const s = start < 0 ? rowCount + start : start;
          const e = stop < 0 ? rowCount + stop : stop;
          const idxs = [];
          for (let i = s; step > 0 ? i < e : i > e; i += step) idxs.push(i);
          const sliced = {};
          for (const c of columns) sliced[c] = idxs.map((i) => colData[c][i]);
          return DataFrame(sliced, { columns: [...columns], index: idxs.map((i) => index[i]) });
        }
      }
      if (typeof row === "number" && typeof colArg === "number") {
        const r = row < 0 ? rowCount + row : row;
        const ci = colArg < 0 ? columns.length + colArg : colArg;
        return colData[columns[ci]][r];
      }
      return void 0;
    },
    // label-based indexing
    loc(row, colArg) {
      if (colArg === void 0) {
        if (!Array.isArray(row) && typeof row !== "object") {
          const r = index.indexOf(row);
          if (r === -1) return void 0;
          const obj = {};
          for (const c of columns) obj[c] = colData[c][r];
          return obj;
        }
        if (Array.isArray(row) && row.length === rowCount && typeof row[0] === "boolean") {
          const sliced = {};
          const idxs = [];
          for (let i = 0; i < rowCount; i++) {
            if (row[i]) idxs.push(i);
          }
          for (const c of columns) sliced[c] = idxs.map((i) => colData[c][i]);
          return DataFrame(sliced, { columns: [...columns], index: idxs.map((i) => index[i]) });
        }
        if (Array.isArray(row)) {
          const positions = row.map((l) => index.indexOf(l));
          const sliced = {};
          for (const c of columns) sliced[c] = positions.map((p) => colData[c][p]);
          return DataFrame(sliced, { columns: [...columns], index: row });
        }
      }
      if (typeof colArg === "string") {
        const r = index.indexOf(row);
        if (r === -1) return void 0;
        return colData[colArg][r];
      }
      if (Array.isArray(colArg)) {
        const r = index.indexOf(row);
        if (r === -1) return void 0;
        const obj = {};
        for (const c of colArg) obj[c] = colData[c][r];
        return obj;
      }
      return void 0;
    },
    // single scalar by integer position
    iat(row, col) {
      const r = row < 0 ? rowCount + row : row;
      const ci = col < 0 ? columns.length + col : col;
      return colData[columns[ci]][r];
    },
    // single scalar by label
    at(row, col) {
      const r = index.indexOf(row);
      if (r === -1) return void 0;
      return colData[col][r];
    },
    // select columns by name(s)
    select(cols) {
      if (typeof cols === "string") {
        return Series(colData[cols], { name: cols, index: [...index] });
      }
      if (Array.isArray(cols)) {
        const sliced = {};
        for (const c of cols) sliced[c] = [...colData[c]];
        return DataFrame(sliced, { columns: [...cols], index: [...index] });
      }
      return void 0;
    },
    // --- Computation & Aggregation ---
    sum() {
      const result = {};
      for (const c of columns) {
        let s = 0;
        for (const v of colData[c]) if (v !== null && v !== void 0 && typeof v === "number") s += v;
        result[c] = s;
      }
      return Series(columns.map((c) => result[c]), { index: [...columns] });
    },
    mean() {
      const result = {};
      for (const c of columns) {
        let s = 0, cnt = 0;
        for (const v of colData[c]) if (v !== null && v !== void 0 && typeof v === "number") {
          s += v;
          cnt++;
        }
        result[c] = cnt > 0 ? s / cnt : NaN;
      }
      return Series(columns.map((c) => result[c]), { index: [...columns] });
    },
    median() {
      const result = {};
      for (const c of columns) {
        const nums = colData[c].filter((v) => v !== null && v !== void 0 && typeof v === "number").sort((a, b) => a - b);
        if (nums.length === 0) {
          result[c] = NaN;
          continue;
        }
        const mid = Math.floor(nums.length / 2);
        result[c] = nums.length % 2 === 0 ? (nums[mid - 1] + nums[mid]) / 2 : nums[mid];
      }
      return Series(columns.map((c) => result[c]), { index: [...columns] });
    },
    min() {
      const result = {};
      for (const c of columns) {
        let m = Infinity;
        for (const v of colData[c]) if (v !== null && v !== void 0 && typeof v === "number" && v < m) m = v;
        result[c] = m === Infinity ? NaN : m;
      }
      return Series(columns.map((c) => result[c]), { index: [...columns] });
    },
    max() {
      const result = {};
      for (const c of columns) {
        let m = -Infinity;
        for (const v of colData[c]) if (v !== null && v !== void 0 && typeof v === "number" && v > m) m = v;
        result[c] = m === -Infinity ? NaN : m;
      }
      return Series(columns.map((c) => result[c]), { index: [...columns] });
    },
    std(ddof) {
      const d = ddof === void 0 ? 1 : ddof;
      const result = {};
      for (const c of columns) {
        let s = 0, s2 = 0, cnt = 0;
        for (const v of colData[c]) {
          if (v !== null && v !== void 0 && typeof v === "number") {
            s += v;
            s2 += v * v;
            cnt++;
          }
        }
        if (cnt <= d) {
          result[c] = NaN;
          continue;
        }
        const mean = s / cnt;
        result[c] = Math.sqrt((s2 - cnt * mean * mean) / (cnt - d));
      }
      return Series(columns.map((c) => result[c]), { index: [...columns] });
    },
    count() {
      const result = {};
      for (const c of columns) {
        let cnt = 0;
        for (const v of colData[c]) if (v !== null && v !== void 0) cnt++;
        result[c] = cnt;
      }
      return Series(columns.map((c) => result[c]), { index: [...columns] });
    },
    prod() {
      const result = {};
      for (const c of columns) {
        let p = 1;
        for (const v of colData[c]) if (v !== null && v !== void 0 && typeof v === "number") p *= v;
        result[c] = p;
      }
      return Series(columns.map((c) => result[c]), { index: [...columns] });
    },
    quantile(q) {
      const result = {};
      for (const c of columns) {
        const nums = colData[c].filter((v) => v !== null && v !== void 0 && typeof v === "number").sort((a, b) => a - b);
        if (nums.length === 0) {
          result[c] = NaN;
          continue;
        }
        const pos = (nums.length - 1) * q;
        const lo = Math.floor(pos);
        const hi = Math.ceil(pos);
        result[c] = lo === hi ? nums[lo] : nums[lo] + (nums[hi] - nums[lo]) * (pos - lo);
      }
      return Series(columns.map((c) => result[c]), { index: [...columns] });
    },
    rolling(windowSize, options2) {
      const opts2 = options2 || {};
      const mp = opts2.minPeriods;
      const numCols = columns.filter((c) => inferDtype(colData[c]) === "int64" || inferDtype(colData[c]) === "float64");
      function makeAgg(fn) {
        const resultData = {};
        for (const c of numCols) {
          resultData[c] = windowAgg(colData[c], windowSize, mp, fn);
        }
        return DataFrame(resultData, { columns: numCols, index: [...index] });
      }
      return {
        sum() {
          return makeAgg(aggSum);
        },
        mean() {
          return makeAgg(aggMean);
        },
        min() {
          return makeAgg(aggMin);
        },
        max() {
          return makeAgg(aggMax);
        },
        std() {
          return makeAgg(aggStd);
        },
        median() {
          return makeAgg(aggMedian);
        }
      };
    },
    expanding(options2) {
      const opts2 = options2 || {};
      const mp = opts2.minPeriods;
      const numCols = columns.filter((c) => inferDtype(colData[c]) === "int64" || inferDtype(colData[c]) === "float64");
      function makeAgg(fn) {
        const resultData = {};
        for (const c of numCols) {
          resultData[c] = expandingAgg(colData[c], mp, fn);
        }
        return DataFrame(resultData, { columns: numCols, index: [...index] });
      }
      return {
        sum() {
          return makeAgg(aggSum);
        },
        mean() {
          return makeAgg(aggMean);
        },
        std() {
          return makeAgg(aggStd);
        }
      };
    },
    astype(dtype) {
      const castFns = {
        "int64": (v) => v === null || v === void 0 ? v : Math.trunc(Number(v)),
        "float64": (v) => v === null || v === void 0 ? v : Number(v),
        "string": (v) => v === null || v === void 0 ? v : String(v),
        "bool": (v) => v === null || v === void 0 ? v : Boolean(v)
      };
      if (typeof dtype === "string") {
        const fn = castFns[dtype];
        const sliced2 = {};
        for (const c of columns) sliced2[c] = colData[c].map(fn);
        return DataFrame(sliced2, { columns: [...columns], index: [...index] });
      }
      const sliced = {};
      for (const c of columns) {
        if (dtype[c]) {
          const fn = castFns[dtype[c]];
          sliced[c] = colData[c].map(fn);
        } else {
          sliced[c] = [...colData[c]];
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    ffill() {
      const sliced = {};
      for (const c of columns) {
        const arr = [...colData[c]];
        for (let i = 1; i < arr.length; i++) {
          if (arr[i] === null || arr[i] === void 0 || typeof arr[i] === "number" && Number.isNaN(arr[i])) {
            arr[i] = arr[i - 1];
          }
        }
        sliced[c] = arr;
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    bfill() {
      const sliced = {};
      for (const c of columns) {
        const arr = [...colData[c]];
        for (let i = arr.length - 2; i >= 0; i--) {
          if (arr[i] === null || arr[i] === void 0 || typeof arr[i] === "number" && Number.isNaN(arr[i])) {
            arr[i] = arr[i + 1];
          }
        }
        sliced[c] = arr;
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    copy() {
      const sliced = {};
      for (const c of columns) sliced[c] = [...colData[c]];
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    var(ddof) {
      const d = ddof === void 0 ? 1 : ddof;
      const result = {};
      for (const c of columns) {
        let s = 0, s2 = 0, cnt = 0;
        for (const v of colData[c]) {
          if (v !== null && v !== void 0 && typeof v === "number") {
            s += v;
            s2 += v * v;
            cnt++;
          }
        }
        if (cnt <= d) {
          result[c] = NaN;
          continue;
        }
        const mean = s / cnt;
        result[c] = (s2 - cnt * mean * mean) / (cnt - d);
      }
      return Series(columns.map((c) => result[c]), { index: [...columns] });
    },
    isna() {
      const sliced = {};
      for (const c of columns) {
        sliced[c] = colData[c].map((v) => v === null || v === void 0 || typeof v === "number" && Number.isNaN(v));
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    notna() {
      const sliced = {};
      for (const c of columns) {
        sliced[c] = colData[c].map((v) => v !== null && v !== void 0 && !(typeof v === "number" && Number.isNaN(v)));
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    any(options2) {
      const axis = options2 && options2.axis !== void 0 ? options2.axis : 0;
      if (axis === 0) {
        const result2 = {};
        for (const c of columns) {
          result2[c] = colData[c].some((v) => Boolean(v));
        }
        return Series(columns.map((c) => result2[c]), { index: [...columns] });
      }
      const result = [];
      for (let i = 0; i < rowCount; i++) {
        result.push(columns.some((c) => Boolean(colData[c][i])));
      }
      return Series(result, { index: [...index] });
    },
    all(options2) {
      const axis = options2 && options2.axis !== void 0 ? options2.axis : 0;
      if (axis === 0) {
        const result2 = {};
        for (const c of columns) {
          result2[c] = colData[c].every((v) => Boolean(v));
        }
        return Series(columns.map((c) => result2[c]), { index: [...columns] });
      }
      const result = [];
      for (let i = 0; i < rowCount; i++) {
        result.push(columns.every((c) => Boolean(colData[c][i])));
      }
      return Series(result, { index: [...index] });
    },
    pipe(fn) {
      return fn(df);
    },
    applymap(fn) {
      const sliced = {};
      for (const c of columns) {
        sliced[c] = colData[c].map(fn);
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    stack() {
      const resultData = { index: [], variable: [], value: [] };
      for (let i = 0; i < rowCount; i++) {
        for (const c of columns) {
          resultData.index.push(index[i]);
          resultData.variable.push(c);
          resultData.value.push(colData[c][i]);
        }
      }
      return Series(resultData.value, {
        index: resultData.index.map((idx, i) => idx + "\0" + resultData.variable[i]),
        name: null
      });
    },
    sample(options2) {
      const opts2 = options2 || {};
      const n = opts2.n || (opts2.frac ? Math.floor(rowCount * opts2.frac) : 1);
      const seed = opts2.randomState;
      let rng = seed !== void 0 ? /* @__PURE__ */ (() => {
        let s = seed;
        return () => {
          s = s * 1103515245 + 12345 & 2147483647;
          return s / 2147483647;
        };
      })() : Math.random;
      const idxs = Array.from({ length: rowCount }, (_, i) => i);
      for (let i = idxs.length - 1; i > 0; i--) {
        const j = Math.floor(rng() * (i + 1));
        const tmp = idxs[i];
        idxs[i] = idxs[j];
        idxs[j] = tmp;
      }
      const sel = idxs.slice(0, n);
      const sliced = {};
      for (const c of columns) sliced[c] = sel.map((i) => colData[c][i]);
      return DataFrame(sliced, { columns: [...columns], index: sel.map((i) => index[i]) });
    },
    describe() {
      const numCols = columns.filter((c) => {
        const dt = inferDtype(colData[c]);
        return dt === "int64" || dt === "float64";
      });
      const stats = ["count", "mean", "std", "min", "25%", "50%", "75%", "max"];
      const result = {};
      for (const c of numCols) {
        const nums = colData[c].filter((v) => v !== null && v !== void 0).sort((a, b) => a - b);
        const n = nums.length;
        const s = nums.reduce((a, b) => a + b, 0);
        const mean = s / n;
        const s2 = nums.reduce((a, v) => a + v * v, 0);
        const std = n > 1 ? Math.sqrt((s2 - n * mean * mean) / (n - 1)) : NaN;
        const percentile = (p) => {
          const pos = (n - 1) * p;
          const lo = Math.floor(pos);
          const hi = Math.ceil(pos);
          if (lo === hi) return nums[lo];
          return nums[lo] + (nums[hi] - nums[lo]) * (pos - lo);
        };
        result[c] = [n, mean, std, nums[0], percentile(0.25), percentile(0.5), percentile(0.75), nums[n - 1]];
      }
      const descData = {};
      for (const c of numCols) {
        descData[c] = result[c];
      }
      return DataFrame(descData, { columns: numCols, index: stats });
    },
    apply(fn, options2) {
      const axis = options2 && options2.axis !== void 0 ? options2.axis : 0;
      if (axis === 0) {
        const result2 = {};
        for (const c of columns) {
          result2[c] = fn(Series(colData[c], { name: c, index: [...index] }));
        }
        return Series(columns.map((c) => result2[c]), { index: [...columns] });
      }
      const result = [];
      for (let i = 0; i < rowCount; i++) {
        const row = {};
        for (const c of columns) row[c] = colData[c][i];
        result.push(fn(row));
      }
      return Series(result, { index: [...index] });
    },
    // filter rows by boolean mask
    filter(mask) {
      const sliced = {};
      const idxs = [];
      for (let i = 0; i < rowCount; i++) {
        if (mask[i]) idxs.push(i);
      }
      for (const c of columns) sliced[c] = idxs.map((i) => colData[c][i]);
      return DataFrame(sliced, { columns: [...columns], index: idxs.map((i) => index[i]) });
    },
    // --- Reshaping & Combining ---
    sortValues(by, options2) {
      const ascending = !options2 || options2.ascending === void 0 ? true : options2.ascending;
      const order = Array.from({ length: rowCount }, (_, i) => i);
      order.sort((a, b) => {
        const va = colData[by][a];
        const vb = colData[by][b];
        if (va < vb) return ascending ? -1 : 1;
        if (va > vb) return ascending ? 1 : -1;
        return 0;
      });
      const sliced = {};
      for (const c of columns) sliced[c] = order.map((i) => colData[c][i]);
      return DataFrame(sliced, { columns: [...columns], index: order.map((i) => index[i]) });
    },
    sortIndex(options2) {
      const ascending = !options2 || options2.ascending === void 0 ? true : options2.ascending;
      const order = Array.from({ length: rowCount }, (_, i) => i);
      order.sort((a, b) => {
        if (index[a] < index[b]) return ascending ? -1 : 1;
        if (index[a] > index[b]) return ascending ? 1 : -1;
        return 0;
      });
      const sliced = {};
      for (const c of columns) sliced[c] = order.map((i) => colData[c][i]);
      return DataFrame(sliced, { columns: [...columns], index: order.map((i) => index[i]) });
    },
    drop(labels, options2) {
      const axis = options2 && options2.axis !== void 0 ? options2.axis : 0;
      const dropList = Array.isArray(labels) ? labels : [labels];
      if (axis === 1) {
        const dropSet2 = new Set(dropList);
        const newCols = columns.filter((c) => !dropSet2.has(c));
        const sliced2 = {};
        for (const c of newCols) sliced2[c] = [...colData[c]];
        return DataFrame(sliced2, { columns: newCols, index: [...index] });
      }
      const dropSet = new Set(dropList);
      const idxs = [];
      for (let i = 0; i < rowCount; i++) {
        if (!dropSet.has(index[i])) idxs.push(i);
      }
      const sliced = {};
      for (const c of columns) sliced[c] = idxs.map((i) => colData[c][i]);
      return DataFrame(sliced, { columns: [...columns], index: idxs.map((i) => index[i]) });
    },
    rename(options2) {
      const { cols } = options2;
      if (cols) {
        const newCols = columns.map((c) => cols[c] !== void 0 ? cols[c] : c);
        const sliced = {};
        for (let i = 0; i < columns.length; i++) {
          sliced[newCols[i]] = [...colData[columns[i]]];
        }
        return DataFrame(sliced, { columns: newCols, index: [...index] });
      }
      return DataFrame({ ...colData }, { columns: [...columns], index: [...index] });
    },
    assign(newCols) {
      const sliced = {};
      for (const c of columns) sliced[c] = [...colData[c]];
      const allCols = [...columns];
      for (const [k, v] of Object.entries(newCols)) {
        if (!allCols.includes(k)) allCols.push(k);
        sliced[k] = typeof v === "function" ? Array.from({ length: rowCount }, (_, i) => {
          const row = {};
          for (const c of columns) row[c] = colData[c][i];
          return v(row);
        }) : [...v];
      }
      return DataFrame(sliced, { columns: allCols, index: [...index] });
    },
    dropna() {
      const idxs = [];
      for (let i = 0; i < rowCount; i++) {
        let hasNull = false;
        for (const c of columns) {
          const v = colData[c][i];
          if (v === null || v === void 0 || typeof v === "number" && Number.isNaN(v)) {
            hasNull = true;
            break;
          }
        }
        if (!hasNull) idxs.push(i);
      }
      const sliced = {};
      for (const c of columns) sliced[c] = idxs.map((i) => colData[c][i]);
      return DataFrame(sliced, { columns: [...columns], index: idxs.map((i) => index[i]) });
    },
    fillna(value) {
      const sliced = {};
      for (const c of columns) {
        sliced[c] = colData[c].map((v) => v === null || v === void 0 || typeof v === "number" && Number.isNaN(v) ? value : v);
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    get T() {
      const newCols = [...index];
      const newIndex = [...columns];
      const sliced = {};
      for (let i = 0; i < newCols.length; i++) {
        const colName = String(newCols[i]);
        sliced[colName] = columns.map((c) => colData[c][i]);
      }
      return DataFrame(sliced, { columns: newCols.map(String), index: newIndex });
    },
    resetIndex(options2) {
      const drop = options2 && options2.drop;
      if (drop) {
        const sliced2 = {};
        for (const c of columns) sliced2[c] = [...colData[c]];
        return DataFrame(sliced2, { columns: [...columns], index: Array.from({ length: rowCount }, (_, i) => i) });
      }
      const sliced = { index: [...index] };
      for (const c of columns) sliced[c] = [...colData[c]];
      return DataFrame(sliced, { columns: ["index", ...columns] });
    },
    setIndex(col) {
      const newIndex = [...colData[col]];
      const newCols = columns.filter((c) => c !== col);
      const sliced = {};
      for (const c of newCols) sliced[c] = [...colData[c]];
      return DataFrame(sliced, { columns: newCols, index: newIndex });
    },
    groupby(by) {
      return GroupBy(colData, columns, index, rowCount, by, DataFrame);
    },
    merge(other, options2) {
      const { on, how = "inner", suffixes } = options2;
      const onArr = Array.isArray(on) ? on : [on];
      const sf = suffixes || ["_x", "_y"];
      const otherCols = other.columns.filter((c) => !onArr.includes(c));
      const leftOnly = columns.filter((c) => !onArr.includes(c));
      const conflicts = new Set(leftOnly.filter((c) => otherCols.includes(c)));
      const allCols = [];
      const leftMap = {};
      const rightMap = {};
      for (const c of columns) {
        const outName = conflicts.has(c) ? c + sf[0] : c;
        allCols.push(outName);
        leftMap[outName] = c;
      }
      for (const c of otherCols) {
        const outName = conflicts.has(c) ? c + sf[1] : c;
        allCols.push(outName);
        rightMap[outName] = c;
      }
      const resultData = {};
      for (const c of allCols) resultData[c] = [];
      function makeKey(row, cols) {
        return cols.map((c) => row[c]).join("\0");
      }
      const otherMap = {};
      const otherRows = other.toJSON();
      const otherUsed = /* @__PURE__ */ new Set();
      for (let ri = 0; ri < otherRows.length; ri++) {
        const key = makeKey(otherRows[ri], onArr);
        if (!otherMap[key]) otherMap[key] = [];
        otherMap[key].push(ri);
      }
      function pushLeft(i) {
        for (const [outName, srcCol] of Object.entries(leftMap)) {
          resultData[outName].push(colData[srcCol][i]);
        }
      }
      function pushLeftNull() {
        for (const outName of Object.keys(leftMap)) {
          resultData[outName].push(null);
        }
      }
      function pushRight(row) {
        for (const [outName, srcCol] of Object.entries(rightMap)) {
          resultData[outName].push(row[srcCol]);
        }
      }
      function pushRightNull() {
        for (const outName of Object.keys(rightMap)) {
          resultData[outName].push(null);
        }
      }
      if (how === "inner" || how === "left") {
        for (let i = 0; i < rowCount; i++) {
          const key = onArr.map((c) => colData[c][i]).join("\0");
          const matches = otherMap[key];
          if (matches) {
            for (const ri of matches) {
              pushLeft(i);
              pushRight(otherRows[ri]);
              otherUsed.add(ri);
            }
          } else if (how === "left") {
            pushLeft(i);
            pushRightNull();
          }
        }
      }
      if (how === "right") {
        const leftMap2 = {};
        for (let i = 0; i < rowCount; i++) {
          const key = onArr.map((c) => colData[c][i]).join("\0");
          if (!leftMap2[key]) leftMap2[key] = [];
          leftMap2[key].push(i);
        }
        for (let ri = 0; ri < otherRows.length; ri++) {
          const key = makeKey(otherRows[ri], onArr);
          const leftMatches = leftMap2[key];
          if (leftMatches) {
            for (const li of leftMatches) {
              pushLeft(li);
              pushRight(otherRows[ri]);
            }
          } else {
            for (const [outName, srcCol] of Object.entries(leftMap)) {
              if (onArr.includes(srcCol)) {
                resultData[outName].push(otherRows[ri][srcCol]);
              } else {
                resultData[outName].push(null);
              }
            }
            pushRight(otherRows[ri]);
          }
        }
      }
      if (how === "outer") {
        for (let i = 0; i < rowCount; i++) {
          const key = onArr.map((c) => colData[c][i]).join("\0");
          const matches = otherMap[key];
          if (matches) {
            for (const ri of matches) {
              pushLeft(i);
              pushRight(otherRows[ri]);
              otherUsed.add(ri);
            }
          } else {
            pushLeft(i);
            pushRightNull();
          }
        }
        for (let ri = 0; ri < otherRows.length; ri++) {
          if (otherUsed.has(ri)) continue;
          for (const [outName, srcCol] of Object.entries(leftMap)) {
            if (onArr.includes(srcCol)) {
              resultData[outName].push(otherRows[ri][srcCol]);
            } else {
              resultData[outName].push(null);
            }
          }
          pushRight(otherRows[ri]);
        }
      }
      return DataFrame(resultData, { columns: allCols });
    },
    melt(options2) {
      const { idVars, valueVars } = options2;
      const vVars = valueVars || columns.filter((c) => !idVars.includes(c));
      const resultData = {};
      const meltCols = [...idVars, "variable", "value"];
      for (const c of meltCols) resultData[c] = [];
      for (const v of vVars) {
        for (let i = 0; i < rowCount; i++) {
          for (const id of idVars) resultData[id].push(colData[id][i]);
          resultData["variable"].push(v);
          resultData["value"].push(colData[v][i]);
        }
      }
      return DataFrame(resultData, { columns: meltCols });
    },
    pivot(options2) {
      const { index: pivotIdx, cols: pivotCols, values: pivotVals } = options2;
      const uniqueIdx = [];
      const seenIdx = /* @__PURE__ */ new Set();
      const uniqueCols = [];
      const seenCols = /* @__PURE__ */ new Set();
      for (let i = 0; i < rowCount; i++) {
        const iv = colData[pivotIdx][i];
        const cv = colData[pivotCols][i];
        if (!seenIdx.has(iv)) {
          uniqueIdx.push(iv);
          seenIdx.add(iv);
        }
        if (!seenCols.has(cv)) {
          uniqueCols.push(cv);
          seenCols.add(cv);
        }
      }
      const resultData = {};
      for (const c of uniqueCols) resultData[String(c)] = new Array(uniqueIdx.length).fill(null);
      for (let i = 0; i < rowCount; i++) {
        const ri = uniqueIdx.indexOf(colData[pivotIdx][i]);
        const ci = String(colData[pivotCols][i]);
        resultData[ci][ri] = colData[pivotVals][i];
      }
      return DataFrame(resultData, { columns: uniqueCols.map(String), index: uniqueIdx });
    },
    // duplicated
    duplicated(options2) {
      const subset = options2 && options2.subset || columns;
      const result = new Array(rowCount).fill(false);
      const seen = /* @__PURE__ */ new Set();
      for (let i = 0; i < rowCount; i++) {
        const key = subset.map((c) => colData[c][i]).join("\0");
        if (seen.has(key)) {
          result[i] = true;
        } else {
          seen.add(key);
        }
      }
      return result;
    },
    // drop_duplicates
    dropDuplicates(options2) {
      const duped = df.duplicated(options2);
      const idxs = [];
      for (let i = 0; i < rowCount; i++) {
        if (!duped[i]) idxs.push(i);
      }
      const sliced = {};
      for (const c of columns) sliced[c] = idxs.map((i) => colData[c][i]);
      return DataFrame(sliced, { columns: [...columns], index: idxs.map((i) => index[i]) });
    },
    // nlargest
    nlargest(n, col) {
      const order = Array.from({ length: rowCount }, (_, i) => i);
      order.sort((a, b) => colData[col][b] - colData[col][a] || a - b);
      const top = order.slice(0, n);
      const sliced = {};
      for (const c of columns) sliced[c] = top.map((i) => colData[c][i]);
      return DataFrame(sliced, { columns: [...columns], index: top.map((i) => index[i]) });
    },
    // nsmallest
    nsmallest(n, col) {
      const order = Array.from({ length: rowCount }, (_, i) => i);
      order.sort((a, b) => colData[col][a] - colData[col][b] || a - b);
      const top = order.slice(0, n);
      const sliced = {};
      for (const c of columns) sliced[c] = top.map((i) => colData[c][i]);
      return DataFrame(sliced, { columns: [...columns], index: top.map((i) => index[i]) });
    },
    // replace
    replace(toReplace, value) {
      const sliced = {};
      if (typeof toReplace === "object" && !Array.isArray(toReplace) && value === void 0) {
        for (const c of columns) sliced[c] = [...colData[c]];
        for (const [col, mapping] of Object.entries(toReplace)) {
          if (sliced[col]) {
            sliced[col] = sliced[col].map((v) => mapping[v] !== void 0 ? mapping[v] : v);
          }
        }
      } else {
        for (const c of columns) {
          sliced[c] = colData[c].map((v) => v === toReplace ? value : v);
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    // where: keep values where mask is true, else NaN
    where(mask) {
      const sliced = {};
      for (const c of columns) {
        sliced[c] = colData[c].map((v, i) => mask[i][columns.indexOf(c)] ? v : NaN);
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    // gt: element-wise > producing 2D boolean array
    gt(other) {
      return Array.from(
        { length: rowCount },
        (_, i) => columns.map((c) => colData[c][i] > other)
      );
    },
    // pivotTable
    pivotTable(options2) {
      const { index: pivotIdx, cols: pivotCols, values: pivotVals, aggfunc = "sum" } = options2;
      const uniqueIdx = [];
      const seenIdx = /* @__PURE__ */ new Set();
      const uniqueCols = [];
      const seenCols = /* @__PURE__ */ new Set();
      for (let i = 0; i < rowCount; i++) {
        const iv = colData[pivotIdx][i];
        const cv = colData[pivotCols][i];
        if (!seenIdx.has(iv)) {
          uniqueIdx.push(iv);
          seenIdx.add(iv);
        }
        if (!seenCols.has(cv)) {
          uniqueCols.push(cv);
          seenCols.add(cv);
        }
      }
      const cells = {};
      for (const ri of uniqueIdx) {
        cells[ri] = {};
        for (const ci of uniqueCols) cells[ri][ci] = [];
      }
      for (let i = 0; i < rowCount; i++) {
        cells[colData[pivotIdx][i]][colData[pivotCols][i]].push(colData[pivotVals][i]);
      }
      const aggFn = {
        sum: (arr) => arr.reduce((a, b) => a + b, 0),
        mean: (arr) => arr.reduce((a, b) => a + b, 0) / arr.length,
        count: (arr) => arr.length,
        min: (arr) => Math.min(...arr),
        max: (arr) => Math.max(...arr)
      }[aggfunc];
      const resultData = {};
      for (const ci of uniqueCols) {
        resultData[String(ci)] = uniqueIdx.map((ri) => cells[ri][ci].length > 0 ? aggFn(cells[ri][ci]) : null);
      }
      return DataFrame(resultData, { columns: uniqueCols.map(String), index: uniqueIdx });
    },
    // corr
    corr() {
      const numCols = columns.filter((c) => inferDtype(colData[c]) === "int64" || inferDtype(colData[c]) === "float64");
      const means = {};
      for (const c of numCols) {
        let s = 0;
        for (const v of colData[c]) s += v;
        means[c] = s / rowCount;
      }
      const resultData = {};
      for (const c1 of numCols) {
        resultData[c1] = numCols.map((c2) => {
          let sumXY = 0, sumX2 = 0, sumY2 = 0;
          for (let i = 0; i < rowCount; i++) {
            const dx = colData[c1][i] - means[c1];
            const dy = colData[c2][i] - means[c2];
            sumXY += dx * dy;
            sumX2 += dx * dx;
            sumY2 += dy * dy;
          }
          return sumXY / Math.sqrt(sumX2 * sumY2);
        });
      }
      return DataFrame(resultData, { columns: numCols, index: numCols });
    },
    // iterrows
    iterrows() {
      const rows = [];
      for (let i = 0; i < rowCount; i++) {
        const vals = columns.map((c) => colData[c][i]);
        rows.push([index[i], vals]);
      }
      return rows;
    },
    // query: simple expression filter (supports "col > val", "col == val", etc)
    query(expr) {
      const match = expr.match(/^\s*(\w+)\s*(>=|<=|!=|==|>|<)\s*(.+)\s*$/);
      if (!match) return df;
      const [, col, op, rawVal] = match;
      const val = isNaN(Number(rawVal)) ? rawVal.replace(/['"]/g, "") : Number(rawVal);
      const ops = {
        ">": (a, b) => a > b,
        "<": (a, b) => a < b,
        ">=": (a, b) => a >= b,
        "<=": (a, b) => a <= b,
        "==": (a, b) => a === b,
        "!=": (a, b) => a !== b
      };
      const fn = ops[op];
      const idxs = [];
      for (let i = 0; i < rowCount; i++) {
        if (fn(colData[col][i], val)) idxs.push(i);
      }
      const sliced = {};
      for (const c of columns) sliced[c] = idxs.map((i) => colData[c][i]);
      return DataFrame(sliced, { columns: [...columns], index: idxs.map((i) => index[i]) });
    },
    explode(col) {
      const resultData = {};
      const newIndex = [];
      for (const c of columns) resultData[c] = [];
      for (let i = 0; i < rowCount; i++) {
        const val = colData[col][i];
        if (Array.isArray(val)) {
          for (const v of val) {
            for (const c of columns) {
              resultData[c].push(c === col ? v : colData[c][i]);
            }
            newIndex.push(index[i]);
          }
        } else {
          for (const c of columns) resultData[c].push(colData[c][i]);
          newIndex.push(index[i]);
        }
      }
      return DataFrame(resultData, { columns: [...columns], index: newIndex });
    },
    // join on index
    join(other, options2) {
      const opts2 = options2 || {};
      const how = opts2.how || "left";
      const lsuffix = opts2.lsuffix || "";
      const rsuffix = opts2.rsuffix || "_r";
      const otherCols = other.columns;
      const conflicts = new Set(columns.filter((c) => otherCols.includes(c)));
      const allCols = [];
      const leftMap = {};
      for (const c of columns) {
        const n = conflicts.has(c) ? c + lsuffix : c;
        allCols.push(n);
        leftMap[n] = c;
      }
      const rightMap = {};
      for (const c of otherCols) {
        const n = conflicts.has(c) ? c + rsuffix : c;
        allCols.push(n);
        rightMap[n] = c;
      }
      const resultData = {};
      for (const c of allCols) resultData[c] = [];
      const otherIdx = {};
      for (let i = 0; i < other.index.length; i++) otherIdx[other.index[i]] = i;
      if (how === "left" || how === "inner") {
        for (let i = 0; i < rowCount; i++) {
          const ri = otherIdx[index[i]];
          if (ri === void 0 && how === "inner") continue;
          for (const [n, c] of Object.entries(leftMap)) resultData[n].push(colData[c][i]);
          for (const [n, c] of Object.entries(rightMap)) resultData[n].push(ri !== void 0 ? other._colData(c)[ri] : null);
        }
      }
      if (how === "right") {
        const selfIdx = {};
        for (let i = 0; i < rowCount; i++) selfIdx[index[i]] = i;
        for (let i = 0; i < other.index.length; i++) {
          const li = selfIdx[other.index[i]];
          for (const [n, c] of Object.entries(leftMap)) resultData[n].push(li !== void 0 ? colData[c][li] : null);
          for (const [n, c] of Object.entries(rightMap)) resultData[n].push(other._colData(c)[i]);
        }
      }
      if (how === "outer") {
        const seen = /* @__PURE__ */ new Set();
        for (let i = 0; i < rowCount; i++) {
          seen.add(index[i]);
          const ri = otherIdx[index[i]];
          for (const [n, c] of Object.entries(leftMap)) resultData[n].push(colData[c][i]);
          for (const [n, c] of Object.entries(rightMap)) resultData[n].push(ri !== void 0 ? other._colData(c)[ri] : null);
        }
        for (let i = 0; i < other.index.length; i++) {
          if (seen.has(other.index[i])) continue;
          for (const [n] of Object.entries(leftMap)) resultData[n].push(null);
          for (const [n, c] of Object.entries(rightMap)) resultData[n].push(other._colData(c)[i]);
        }
      }
      return DataFrame(resultData, { columns: allCols });
    },
    // insert column at position
    insert(loc, column, value) {
      const newCols = [...columns];
      newCols.splice(loc, 0, column);
      const sliced = {};
      for (const c of columns) sliced[c] = [...colData[c]];
      sliced[column] = Array.isArray(value) ? [...value] : new Array(rowCount).fill(value);
      return DataFrame(sliced, { columns: newCols, index: [...index] });
    },
    // remove and return column as Series
    pop(column) {
      return Series(colData[column], { name: column, index: [...index] });
    },
    // conform to new index, filling gaps with NaN
    reindex(options2) {
      const newIdx = options2.index || index;
      const newCols = options2.columns || columns;
      const oldIdxMap = {};
      for (let i = 0; i < index.length; i++) oldIdxMap[index[i]] = i;
      const sliced = {};
      for (const c of newCols) {
        sliced[c] = newIdx.map((idx) => {
          const ri = oldIdxMap[idx];
          if (ri === void 0) return NaN;
          if (colData[c] === void 0) return NaN;
          return colData[c][ri];
        });
      }
      return DataFrame(sliced, { columns: newCols, index: newIdx });
    },
    // fill nulls from other DataFrame
    combineFirst(other) {
      const allCols = [.../* @__PURE__ */ new Set([...columns, ...other.columns])];
      const allIdx = [.../* @__PURE__ */ new Set([...index, ...other.index])];
      const selfIdxMap = {};
      for (let i = 0; i < index.length; i++) selfIdxMap[index[i]] = i;
      const otherIdxMap = {};
      for (let i = 0; i < other.index.length; i++) otherIdxMap[other.index[i]] = i;
      const sliced = {};
      for (const c of allCols) {
        sliced[c] = allIdx.map((idx) => {
          const si = selfIdxMap[idx];
          const oi = otherIdxMap[idx];
          const sv = si !== void 0 && colData[c] ? colData[c][si] : null;
          if (sv !== null && sv !== void 0 && !(typeof sv === "number" && Number.isNaN(sv))) return sv;
          if (oi !== void 0 && other._colData(c)) return other._colData(c)[oi];
          return null;
        });
      }
      return DataFrame(sliced, { columns: allCols, index: allIdx });
    },
    // update values from other DataFrame
    update(other) {
      const sliced = {};
      for (const c of columns) sliced[c] = [...colData[c]];
      const otherIdxMap = {};
      for (let i = 0; i < other.index.length; i++) otherIdxMap[other.index[i]] = i;
      for (const c of columns) {
        if (!other._colData(c)) continue;
        for (let i = 0; i < rowCount; i++) {
          const oi = otherIdxMap[index[i]];
          if (oi !== void 0) {
            const ov = other._colData(c)[oi];
            if (ov !== null && ov !== void 0 && !(typeof ov === "number" && Number.isNaN(ov))) {
              sliced[c][i] = ov;
            }
          }
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    // summary info string
    info() {
      const lines = [];
      lines.push(`<class 'DataFrame'>`);
      lines.push(`Index: ${rowCount} entries`);
      lines.push(`Data columns (total ${columns.length} columns):`);
      for (const c of columns) {
        let nonNull = 0;
        for (const v of colData[c]) {
          if (v !== null && v !== void 0 && !(typeof v === "number" && Number.isNaN(v))) nonNull++;
        }
        lines.push(` ${c}    ${nonNull} non-null    ${inferDtype(colData[c])}`);
      }
      return lines.join("\n");
    },
    // --- Cumulative operations (per column) ---
    cumsum() {
      const sliced = {};
      for (const c of columns) {
        const dt = inferDtype(colData[c]);
        if (dt === "int64" || dt === "float64") {
          let s = 0;
          sliced[c] = colData[c].map((v) => {
            if (v !== null && v !== void 0) {
              s += v;
              return s;
            }
            return NaN;
          });
        } else {
          sliced[c] = [...colData[c]];
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    cumprod() {
      const sliced = {};
      for (const c of columns) {
        const dt = inferDtype(colData[c]);
        if (dt === "int64" || dt === "float64") {
          let p = 1;
          sliced[c] = colData[c].map((v) => {
            if (v !== null && v !== void 0) {
              p *= v;
              return p;
            }
            return NaN;
          });
        } else {
          sliced[c] = [...colData[c]];
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    cummin() {
      const sliced = {};
      for (const c of columns) {
        const dt = inferDtype(colData[c]);
        if (dt === "int64" || dt === "float64") {
          let m = Infinity;
          sliced[c] = colData[c].map((v) => {
            if (v !== null && v !== void 0) {
              m = Math.min(m, v);
              return m;
            }
            return NaN;
          });
        } else {
          sliced[c] = [...colData[c]];
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    cummax() {
      const sliced = {};
      for (const c of columns) {
        const dt = inferDtype(colData[c]);
        if (dt === "int64" || dt === "float64") {
          let m = -Infinity;
          sliced[c] = colData[c].map((v) => {
            if (v !== null && v !== void 0) {
              m = Math.max(m, v);
              return m;
            }
            return NaN;
          });
        } else {
          sliced[c] = [...colData[c]];
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    diff(periods) {
      const p = periods === void 0 ? 1 : periods;
      const sliced = {};
      for (const c of columns) {
        const dt = inferDtype(colData[c]);
        if (dt === "int64" || dt === "float64") {
          sliced[c] = colData[c].map((v, i) => {
            if (i < p) return NaN;
            const prev = colData[c][i - p];
            if (v === null || v === void 0 || prev === null || prev === void 0) return NaN;
            return v - prev;
          });
        } else {
          sliced[c] = [...colData[c]];
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    pctChange(periods) {
      const p = periods === void 0 ? 1 : periods;
      const sliced = {};
      for (const c of columns) {
        const dt = inferDtype(colData[c]);
        if (dt === "int64" || dt === "float64") {
          sliced[c] = colData[c].map((v, i) => {
            if (i < p) return NaN;
            const prev = colData[c][i - p];
            if (v === null || v === void 0 || prev === null || prev === void 0 || prev === 0) return NaN;
            return (v - prev) / prev;
          });
        } else {
          sliced[c] = [...colData[c]];
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    clip(options2) {
      const { lower, upper } = options2;
      const sliced = {};
      for (const c of columns) {
        const dt = inferDtype(colData[c]);
        if (dt === "int64" || dt === "float64") {
          sliced[c] = colData[c].map((v) => {
            if (v === null || v === void 0) return v;
            let r = v;
            if (lower !== void 0 && r < lower) r = lower;
            if (upper !== void 0 && r > upper) r = upper;
            return r;
          });
        } else {
          sliced[c] = [...colData[c]];
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    round(decimals) {
      const d = decimals === void 0 ? 0 : decimals;
      const factor = Math.pow(10, d);
      const sliced = {};
      for (const c of columns) {
        const dt = inferDtype(colData[c]);
        if (dt === "int64" || dt === "float64") {
          sliced[c] = colData[c].map((v) => v === null || v === void 0 ? v : Math.round(v * factor) / factor);
        } else {
          sliced[c] = [...colData[c]];
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    rank() {
      const sliced = {};
      for (const c of columns) {
        const dt = inferDtype(colData[c]);
        if (dt === "int64" || dt === "float64") {
          const sorted = colData[c].map((v, i2) => ({ v, i: i2 })).sort((a, b) => a.v - b.v);
          const ranks = new Array(rowCount);
          let i = 0;
          while (i < sorted.length) {
            let j = i;
            while (j < sorted.length && sorted[j].v === sorted[i].v) j++;
            const avgRank = (i + 1 + j) / 2;
            for (let k = i; k < j; k++) ranks[sorted[k].i] = avgRank;
            i = j;
          }
          sliced[c] = ranks;
        } else {
          sliced[c] = [...colData[c]];
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    idxmin() {
      const result = {};
      for (const c of columns) {
        const dt = inferDtype(colData[c]);
        if (dt === "int64" || dt === "float64") {
          let minVal = Infinity, minIdx = null;
          for (let i = 0; i < rowCount; i++) {
            if (colData[c][i] !== null && colData[c][i] !== void 0 && colData[c][i] < minVal) {
              minVal = colData[c][i];
              minIdx = index[i];
            }
          }
          result[c] = minIdx;
        }
      }
      const numCols = columns.filter((c) => inferDtype(colData[c]) === "int64" || inferDtype(colData[c]) === "float64");
      return Series(numCols.map((c) => result[c]), { index: numCols });
    },
    idxmax() {
      const result = {};
      for (const c of columns) {
        const dt = inferDtype(colData[c]);
        if (dt === "int64" || dt === "float64") {
          let maxVal = -Infinity, maxIdx = null;
          for (let i = 0; i < rowCount; i++) {
            if (colData[c][i] !== null && colData[c][i] !== void 0 && colData[c][i] > maxVal) {
              maxVal = colData[c][i];
              maxIdx = index[i];
            }
          }
          result[c] = maxIdx;
        }
      }
      const numCols = columns.filter((c) => inferDtype(colData[c]) === "int64" || inferDtype(colData[c]) === "float64");
      return Series(numCols.map((c) => result[c]), { index: numCols });
    },
    skew() {
      const result = {};
      for (const c of columns) {
        const vals = colData[c].filter((v) => v !== null && v !== void 0 && typeof v === "number");
        const n = vals.length;
        if (n < 3) {
          result[c] = NaN;
          continue;
        }
        const m = vals.reduce((a, b) => a + b, 0) / n;
        let m2 = 0, m3 = 0;
        for (const v of vals) {
          const d = v - m;
          m2 += d * d;
          m3 += d * d * d;
        }
        const variance = m2 / (n - 1);
        const sd = Math.sqrt(variance);
        result[c] = n / ((n - 1) * (n - 2)) * (m3 / (sd * sd * sd));
      }
      const numCols = columns.filter((c) => inferDtype(colData[c]) === "int64" || inferDtype(colData[c]) === "float64");
      return Series(numCols.map((c) => result[c]), { index: numCols });
    },
    kurt() {
      const result = {};
      for (const c of columns) {
        const vals = colData[c].filter((v) => v !== null && v !== void 0 && typeof v === "number");
        const n = vals.length;
        if (n < 4) {
          result[c] = NaN;
          continue;
        }
        const m = vals.reduce((a, b) => a + b, 0) / n;
        let m2 = 0, m4 = 0;
        for (const v of vals) {
          const d = v - m;
          m2 += d * d;
          m4 += d * d * d * d;
        }
        const variance = m2 / (n - 1);
        const num = n * (n + 1) / ((n - 1) * (n - 2) * (n - 3)) * (m4 / (variance * variance));
        const correction = 3 * (n - 1) * (n - 1) / ((n - 2) * (n - 3));
        result[c] = num - correction;
      }
      const numCols = columns.filter((c) => inferDtype(colData[c]) === "int64" || inferDtype(colData[c]) === "float64");
      return Series(numCols.map((c) => result[c]), { index: numCols });
    },
    sem(ddof) {
      const d = ddof === void 0 ? 1 : ddof;
      const result = {};
      for (const c of columns) {
        let s = 0, s2 = 0, cnt = 0;
        for (const v of colData[c]) {
          if (v !== null && v !== void 0 && typeof v === "number") {
            s += v;
            s2 += v * v;
            cnt++;
          }
        }
        if (cnt <= d) {
          result[c] = NaN;
          continue;
        }
        const mean = s / cnt;
        const std = Math.sqrt((s2 - cnt * mean * mean) / (cnt - d));
        result[c] = std / Math.sqrt(cnt);
      }
      return Series(columns.map((c) => result[c]), { index: [...columns] });
    },
    cov() {
      const numCols = columns.filter((c) => inferDtype(colData[c]) === "int64" || inferDtype(colData[c]) === "float64");
      const means = {};
      for (const c of numCols) {
        let s = 0;
        for (const v of colData[c]) s += v;
        means[c] = s / rowCount;
      }
      const resultData = {};
      for (const c1 of numCols) {
        resultData[c1] = numCols.map((c2) => {
          let sumXY = 0;
          for (let i = 0; i < rowCount; i++) {
            sumXY += (colData[c1][i] - means[c1]) * (colData[c2][i] - means[c2]);
          }
          return sumXY / (rowCount - 1);
        });
      }
      return DataFrame(resultData, { columns: numCols, index: numCols });
    },
    map(fn) {
      return df.applymap(fn);
    },
    selectDtypes(options2) {
      const { include, exclude } = options2;
      const selected = columns.filter((c) => {
        const dt = inferDtype(colData[c]);
        if (include) {
          const incl = Array.isArray(include) ? include : [include];
          if (!incl.some((t) => dt.includes(t) || t === "number" && (dt === "int64" || dt === "float64"))) return false;
        }
        if (exclude) {
          const excl = Array.isArray(exclude) ? exclude : [exclude];
          if (excl.some((t) => dt.includes(t) || t === "number" && (dt === "int64" || dt === "float64"))) return false;
        }
        return true;
      });
      const sliced = {};
      for (const c of selected) sliced[c] = [...colData[c]];
      return DataFrame(sliced, { columns: selected, index: [...index] });
    },
    // element-wise arithmetic on numeric columns
    add(other) {
      const scalar = typeof other === "number";
      const sliced = {};
      for (const c of columns) {
        const dt = inferDtype(colData[c]);
        if (dt === "int64" || dt === "float64") {
          const ov = scalar ? other : other._isPandasDataFrame ? other._colData(c) : colData[c];
          sliced[c] = colData[c].map((v, i) => v + (scalar ? other : ov[i]));
        } else {
          sliced[c] = [...colData[c]];
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    sub(other) {
      const scalar = typeof other === "number";
      const sliced = {};
      for (const c of columns) {
        const dt = inferDtype(colData[c]);
        if (dt === "int64" || dt === "float64") {
          const ov = scalar ? other : other._isPandasDataFrame ? other._colData(c) : colData[c];
          sliced[c] = colData[c].map((v, i) => v - (scalar ? other : ov[i]));
        } else {
          sliced[c] = [...colData[c]];
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    mul(other) {
      const scalar = typeof other === "number";
      const sliced = {};
      for (const c of columns) {
        const dt = inferDtype(colData[c]);
        if (dt === "int64" || dt === "float64") {
          const ov = scalar ? other : other._isPandasDataFrame ? other._colData(c) : colData[c];
          sliced[c] = colData[c].map((v, i) => v * (scalar ? other : ov[i]));
        } else {
          sliced[c] = [...colData[c]];
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    div(other) {
      const scalar = typeof other === "number";
      const sliced = {};
      for (const c of columns) {
        const dt = inferDtype(colData[c]);
        if (dt === "int64" || dt === "float64") {
          const ov = scalar ? other : other._isPandasDataFrame ? other._colData(c) : colData[c];
          sliced[c] = colData[c].map((v, i) => v / (scalar ? other : ov[i]));
        } else {
          sliced[c] = [...colData[c]];
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    floordiv(other) {
      const scalar = typeof other === "number";
      const sliced = {};
      for (const c of columns) {
        const dt = inferDtype(colData[c]);
        if (dt === "int64" || dt === "float64") {
          const ov = scalar ? other : other._isPandasDataFrame ? other._colData(c) : colData[c];
          sliced[c] = colData[c].map((v, i) => Math.floor(v / (scalar ? other : ov[i])));
        } else {
          sliced[c] = [...colData[c]];
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    mod(other) {
      const scalar = typeof other === "number";
      const sliced = {};
      for (const c of columns) {
        const dt = inferDtype(colData[c]);
        if (dt === "int64" || dt === "float64") {
          const ov = scalar ? other : other._isPandasDataFrame ? other._colData(c) : colData[c];
          sliced[c] = colData[c].map((v, i) => v % (scalar ? other : ov[i]));
        } else {
          sliced[c] = [...colData[c]];
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    pow(other) {
      const scalar = typeof other === "number";
      const sliced = {};
      for (const c of columns) {
        const dt = inferDtype(colData[c]);
        if (dt === "int64" || dt === "float64") {
          const ov = scalar ? other : other._isPandasDataFrame ? other._colData(c) : colData[c];
          sliced[c] = colData[c].map((v, i) => Math.pow(v, scalar ? other : ov[i]));
        } else {
          sliced[c] = [...colData[c]];
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    abs() {
      const sliced = {};
      for (const c of columns) {
        const dt = inferDtype(colData[c]);
        if (dt === "int64" || dt === "float64") {
          sliced[c] = colData[c].map((v) => v !== null && v !== void 0 ? Math.abs(v) : v);
        } else {
          sliced[c] = [...colData[c]];
        }
      }
      return DataFrame(sliced, { columns: [...columns], index: [...index] });
    },
    // iterate [columnName, Series] pairs
    items() {
      return columns.map((c) => [c, Series(colData[c], { name: c, index: [...index] })]);
    },
    // array of row objects with Index
    itertuples() {
      const rows = [];
      for (let i = 0; i < rowCount; i++) {
        const row = { Index: index[i] };
        for (const c of columns) row[c] = colData[c][i];
        rows.push(row);
      }
      return rows;
    },
    // convert to dict with orient parameter
    toDict(orient) {
      const o = orient || "dict";
      if (o === "list") {
        const result2 = {};
        for (const c of columns) result2[c] = [...colData[c]];
        return result2;
      }
      if (o === "records") {
        return df.toJSON();
      }
      if (o === "index") {
        const result2 = {};
        for (let i = 0; i < rowCount; i++) {
          const row = {};
          for (const c of columns) row[c] = colData[c][i];
          result2[String(index[i])] = row;
        }
        return result2;
      }
      const result = {};
      for (const c of columns) {
        result[c] = {};
        for (let i = 0; i < rowCount; i++) {
          result[c][String(index[i])] = colData[c][i];
        }
      }
      return result;
    },
    // array of {index, ...row}
    toRecords() {
      const rows = [];
      for (let i = 0; i < rowCount; i++) {
        const row = { index: index[i] };
        for (const c of columns) row[c] = colData[c][i];
        rows.push(row);
      }
      return rows;
    },
    // internal access for column data
    _colData(name) {
      return colData[name];
    }
  };
  df.sort_values = df.sortValues;
  df.sort_index = df.sortIndex;
  df.drop_duplicates = df.dropDuplicates;
  df.reset_index = df.resetIndex;
  df.set_index = df.setIndex;
  df.pivot_table = df.pivotTable;
  df.to_json = df.toJSON;
  df.to_dict = df.toDict;
  df.to_records = df.toRecords;
  df.select_dtypes = df.selectDtypes;
  df.pct_change = df.pctChange;
  df.combine_first = df.combineFirst;
  df[Symbol.for("nodejs.util.inspect.custom")] = function() {
    return df.toString();
  };
  return df;
}

// src/io.js
function readCsv(csvString, options) {
  const opts = options || {};
  const sep = opts.sep || ",";
  const header = opts.header !== void 0 ? opts.header : 0;
  const indexCol = opts.indexCol;
  const skipRows = opts.skipRows || 0;
  const nRows = opts.nRows;
  const lines = csvString.trim().split("\n");
  const startLine = skipRows;
  let columns = null;
  let dataStart = startLine;
  if (header !== null) {
    columns = lines[startLine + header].split(sep).map((s) => s.trim());
    dataStart = startLine + header + 1;
  }
  let endLine = lines.length;
  if (nRows !== void 0) endLine = Math.min(dataStart + nRows, lines.length);
  const rows = [];
  for (let i = dataStart; i < endLine; i++) {
    if (lines[i].trim() === "") continue;
    rows.push(lines[i].split(sep).map((s) => s.trim()));
  }
  if (columns === null) {
    columns = rows[0].map((_, i) => i);
  }
  const colData = {};
  for (let ci = 0; ci < columns.length; ci++) {
    colData[columns[ci]] = rows.map((row) => {
      const val = row[ci];
      if (val === "" || val === void 0) return null;
      const num = Number(val);
      if (!isNaN(num) && val !== "") return num;
      if (val === "true" || val === "True") return true;
      if (val === "false" || val === "False") return false;
      return val;
    });
  }
  let idx = void 0;
  let finalCols = [...columns];
  if (indexCol !== void 0) {
    const icName = typeof indexCol === "number" ? columns[indexCol] : indexCol;
    idx = colData[icName];
    finalCols = columns.filter((c) => c !== icName);
    const filteredData = {};
    for (const c of finalCols) filteredData[c] = colData[c];
    return DataFrame(filteredData, { columns: finalCols, index: idx });
  }
  return DataFrame(colData, { columns: finalCols });
}
function toCsv(df, options) {
  const opts = options || {};
  const sep = opts.sep || ",";
  const includeIndex = opts.index !== void 0 ? opts.index : true;
  const includeHeader = opts.header !== void 0 ? opts.header : true;
  const lines = [];
  if (includeHeader) {
    const headerParts = includeIndex ? [""] : [];
    headerParts.push(...df.columns);
    lines.push(headerParts.join(sep));
  }
  const values = df.values;
  for (let i = 0; i < values.length; i++) {
    const parts = includeIndex ? [df.index[i]] : [];
    parts.push(...values[i].map((v) => v === null || v === void 0 ? "" : v));
    lines.push(parts.join(sep));
  }
  return lines.join("\n") + "\n";
}
function readJson(jsonString) {
  const data = JSON.parse(jsonString);
  if (Array.isArray(data)) {
    return DataFrame(data);
  }
  return DataFrame(data);
}
function toJson(df, options) {
  const opts = options || {};
  const orient = opts.orient || "records";
  if (orient === "records") {
    return JSON.stringify(df.toJSON());
  }
  if (orient === "columns") {
    const result = {};
    for (const c of df.columns) {
      const col = df._colData(c);
      const indexed = {};
      for (let i = 0; i < col.length; i++) {
        indexed[String(df.index[i])] = col[i];
      }
      result[c] = indexed;
    }
    return JSON.stringify(result);
  }
  if (orient === "index") {
    const result = {};
    const values = df.values;
    for (let i = 0; i < df.index.length; i++) {
      const row = {};
      for (let j = 0; j < df.columns.length; j++) {
        row[df.columns[j]] = values[i][j];
      }
      result[df.index[i]] = row;
    }
    return JSON.stringify(result);
  }
  return JSON.stringify(df.toJSON());
}
function toNumeric(data, options) {
  const opts = options || {};
  const errors = opts.errors || "raise";
  const values = data._isPandasSeries ? data.values : Array.isArray(data) ? data : [data];
  const result = values.map((v) => {
    if (v === null || v === void 0) return NaN;
    const num = Number(v);
    if (isNaN(num)) {
      if (errors === "coerce") return NaN;
      return v;
    }
    return num;
  });
  if (data._isPandasSeries) {
    return Series(result, { name: data.name, index: [...data.index], dtype: "float64" });
  }
  return result;
}
function getDummies(data, options) {
  const opts = options || {};
  const dropFirst = opts.dropFirst || false;
  const prefix = opts.prefix;
  if (data._isPandasSeries) {
    const vals = data.values;
    const categories = [...new Set(vals)].sort();
    const cats = dropFirst ? categories.slice(1) : categories;
    const resultData = {};
    for (const cat of cats) {
      const colName = prefix ? prefix + "_" + cat : cat;
      resultData[String(colName)] = vals.map((v) => v === cat);
    }
    return DataFrame(resultData, { columns: cats.map((c) => String(prefix ? prefix + "_" + c : c)), index: data.index ? [...data.index] : void 0 });
  }
  if (data._isPandasDataFrame) {
    const targetCols = opts.columns || data.columns.filter((c) => {
      const first = data._colData(c).find((v) => v !== null && v !== void 0);
      return typeof first === "string";
    });
    const resultData = {};
    const resultCols = [];
    for (const c of data.columns) {
      if (targetCols.includes(c)) {
        const vals = data._colData(c);
        const categories = [...new Set(vals)].sort();
        const cats = dropFirst ? categories.slice(1) : categories;
        for (const cat of cats) {
          const colName = c + "_" + cat;
          resultData[colName] = vals.map((v) => v === cat);
          resultCols.push(colName);
        }
      } else {
        resultData[c] = [...data._colData(c)];
        resultCols.push(c);
      }
    }
    return DataFrame(resultData, { columns: resultCols, index: [...data.index] });
  }
}
var read_csv = readCsv;
var to_csv = toCsv;
var read_json = readJson;
var to_json = toJson;
var to_numeric = toNumeric;
var get_dummies = getDummies;

// src/utils.js
function cut(data, bins, options) {
  const opts = options || {};
  const values = data._isPandasSeries ? data.values : data;
  const labels = opts.labels;
  const right = opts.right !== void 0 ? opts.right : true;
  let edges;
  if (typeof bins === "number") {
    const min = Math.min(...values.filter((v) => v !== null && v !== void 0));
    const max = Math.max(...values.filter((v) => v !== null && v !== void 0));
    const width = (max - min) / bins;
    edges = [];
    for (let i = 0; i <= bins; i++) {
      edges.push(min + i * width);
    }
    edges[0] = edges[0] - 1e-3;
  } else {
    edges = bins;
  }
  const result = values.map((v) => {
    if (v === null || v === void 0 || typeof v === "number" && isNaN(v)) return null;
    for (let i = 0; i < edges.length - 1; i++) {
      const lo = edges[i];
      const hi = edges[i + 1];
      const inBin = right ? v > lo && v <= hi : v >= lo && v < hi;
      if (inBin) {
        if (labels) return labels[i];
        if (right) return `(${lo}, ${hi}]`;
        return `[${lo}, ${hi})`;
      }
    }
    return null;
  });
  if (data._isPandasSeries) {
    return Series(result, { name: data.name, index: [...data.index] });
  }
  return result;
}
function qcut(data, q, options) {
  const opts = options || {};
  const values = data._isPandasSeries ? data.values : data;
  const labels = opts.labels;
  const sorted = values.filter((v) => v !== null && v !== void 0).sort((a, b) => a - b);
  const n = sorted.length;
  const edges = [];
  for (let i = 0; i <= q; i++) {
    const pos = (n - 1) * (i / q);
    const lo = Math.floor(pos);
    const hi = Math.ceil(pos);
    if (lo === hi) {
      edges.push(sorted[lo]);
    } else {
      edges.push(sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo));
    }
  }
  edges[0] = edges[0] - 1e-3;
  const result = values.map((v) => {
    if (v === null || v === void 0 || typeof v === "number" && isNaN(v)) return null;
    for (let i = 0; i < edges.length - 1; i++) {
      if (v > edges[i] && v <= edges[i + 1]) {
        if (labels) return labels[i];
        return `(${edges[i]}, ${edges[i + 1]}]`;
      }
    }
    return null;
  });
  if (data._isPandasSeries) {
    return Series(result, { name: data.name, index: [...data.index] });
  }
  return result;
}

// src/bridge.js
function transpile(pyCode) {
  const lines = pyCode.split("\n");
  const jsLines = [];
  for (let line of lines) {
    const trimmed = line.trim();
    if (trimmed === "" || /^import\s+pandas/.test(trimmed) || /^from\s+pandas/.test(trimmed)) {
      jsLines.push("");
      continue;
    }
    if (trimmed.startsWith("#")) {
      jsLines.push(trimmed.replace(/^#/, "//"));
      continue;
    }
    let out = line;
    out = out.replace(/\bTrue\b/g, "true");
    out = out.replace(/\bFalse\b/g, "false");
    out = out.replace(/\bNone\b/g, "null");
    out = out.replace(/\bprint\s*\(/g, "console.log(");
    out = out.replace(/\{(\s*)'(\w+)'\s*:/g, "{$1$2:");
    out = out.replace(/,(\s*)'(\w+)'\s*:/g, ",$1$2:");
    const assignMatch = out.match(/^(\s*)([a-zA-Z_]\w*)\s*=\s*(.+)$/);
    if (assignMatch) {
      const [, indent, varName, expr] = assignMatch;
      if (!/[=!<>]=/.test(expr.charAt(0))) {
        out = `${indent}let ${varName} = ${expr}`;
      }
    }
    out = out.replace(/\.iloc\[([^\]]+)\]/g, ".iloc($1)");
    out = out.replace(/\.loc\[([^\]]+)\]/g, ".loc($1)");
    out = out.replace(/(\w)\[['"](\w+)['"]\]/g, "$1.col('$2')");
    out = out.replace(/\blen\((\w+)\)/g, "$1.length");
    out = out.replace(/\.iloc\((\d+):(\d+)\)/g, ".iloc($1, $2)");
    jsLines.push(out);
  }
  let lastIdx = -1;
  for (let i = jsLines.length - 1; i >= 0; i--) {
    if (jsLines[i].trim() !== "") {
      lastIdx = i;
      break;
    }
  }
  if (lastIdx >= 0) {
    const last = jsLines[lastIdx].trim();
    if (!last.startsWith("let ") && !last.startsWith("const ") && !last.startsWith("console.")) {
      jsLines[lastIdx] = jsLines[lastIdx].replace(last, `return ${last}`);
    }
  }
  return jsLines.join("\n");
}
function run(pyCode, pd2) {
  const jsCode = transpile(pyCode);
  const fn = new Function("pd", jsCode);
  return fn(pd2);
}

// src/index.js
setDataFrame(DataFrame);
function isna(value) {
  return value === null || value === void 0 || typeof value === "number" && Number.isNaN(value);
}
function notna(value) {
  return !isna(value);
}
function concat(items, options) {
  const opts = options || {};
  const ignoreIndex = opts.ignoreIndex || false;
  const axis = opts.axis || 0;
  if (axis === 1) {
    const baseIndex = [...items[0].index];
    const resultData2 = {};
    const resultCols = [];
    for (const df of items) {
      if (df._isPandasSeries) {
        const colName = df.name || resultCols.length;
        resultData2[colName] = [...df.values];
        resultCols.push(colName);
      } else {
        for (const c of df.columns) {
          resultData2[c] = [...df._colData(c)];
          resultCols.push(c);
        }
      }
    }
    return DataFrame(resultData2, { columns: resultCols, index: baseIndex });
  }
  if (items[0]._isPandasSeries) {
    let vals = [];
    let idxs = [];
    for (const s of items) {
      vals = vals.concat(s.values);
      idxs = idxs.concat(s.index);
    }
    if (ignoreIndex) idxs = vals.map((_, i) => i);
    return Series(vals, { index: idxs });
  }
  const allCols = /* @__PURE__ */ new Set();
  for (const df of items) {
    for (const c of df.columns) allCols.add(c);
  }
  const cols = [...allCols];
  const resultData = {};
  for (const c of cols) resultData[c] = [];
  let allIndex = [];
  for (const df of items) {
    const n = df.shape[0];
    allIndex = allIndex.concat(df.index);
    for (const c of cols) {
      const cd = df._colData(c);
      if (cd) {
        for (const v of cd) resultData[c].push(v);
      } else {
        for (let i = 0; i < n; i++) resultData[c].push(null);
      }
    }
  }
  if (ignoreIndex) allIndex = resultData[cols[0]].map((_, i) => i);
  return DataFrame(resultData, { columns: cols, index: allIndex });
}
function merge(left, right, options) {
  return left.merge(right, options);
}
function unique(values) {
  const arr = values._isPandasSeries ? values.values : values;
  return [...new Set(arr)];
}
function factorize(values) {
  const arr = values._isPandasSeries ? values.values : values;
  const uniques = [];
  const seen = {};
  const codes = arr.map((v) => {
    if (seen[v] === void 0) {
      seen[v] = uniques.length;
      uniques.push(v);
    }
    return seen[v];
  });
  return [codes, uniques];
}
function pivotTable(df, options) {
  return df.pivotTable(options);
}
function melt(df, options) {
  return df.melt(options);
}
function crosstab(idx, col) {
  const idxVals = idx._isPandasSeries ? idx.values : idx;
  const colVals = col._isPandasSeries ? col.values : col;
  const rowKeys = [], colKeys = [];
  const rowSeen = /* @__PURE__ */ new Set(), colSeen = /* @__PURE__ */ new Set();
  for (let i = 0; i < idxVals.length; i++) {
    if (!rowSeen.has(idxVals[i])) {
      rowKeys.push(idxVals[i]);
      rowSeen.add(idxVals[i]);
    }
    if (!colSeen.has(colVals[i])) {
      colKeys.push(colVals[i]);
      colSeen.add(colVals[i]);
    }
  }
  const resultData = {};
  for (const ck of colKeys) resultData[String(ck)] = new Array(rowKeys.length).fill(0);
  for (let i = 0; i < idxVals.length; i++) {
    const ri = rowKeys.indexOf(idxVals[i]);
    resultData[String(colVals[i])][ri]++;
  }
  return DataFrame(resultData, { columns: colKeys.map(String), index: rowKeys });
}
var pd = {
  Series,
  DataFrame,
  concat,
  cut,
  qcut,
  transpile,
  isna,
  notna,
  isnull: isna,
  notnull: notna,
  merge,
  unique,
  factorize,
  crosstab,
  pivotTable,
  pivot_table: pivotTable,
  melt,
  readCsv,
  toCsv,
  readJson,
  toJson,
  toNumeric,
  getDummies,
  toDatetime,
  dateRange,
  read_csv,
  to_csv,
  read_json,
  to_json,
  to_numeric,
  get_dummies,
  to_datetime,
  date_range
};
pd.run = (pyCode) => run(pyCode, pd);
var index_default = pd;
