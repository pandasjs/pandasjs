// Window operations - rolling, expanding, ewm
import { Series } from './series.js'

function isValid(v) {
    return v !== null && v !== undefined && !Number.isNaN(v)
}

function windowAgg(values, windowSize, minPeriods, fn) {
    const mp = (minPeriods === undefined) ? windowSize : minPeriods
    const result = new Array(values.length)
    for (let i = 0; i < values.length; i++) {
        const start = Math.max(0, i - windowSize + 1)
        const win = []
        for (let j = start; j <= i; j++) {
            if (isValid(values[j])) win.push(values[j])
        }
        result[i] = win.length >= mp ? fn(win) : NaN
    }
    return result
}

function expandingAgg(values, minPeriods, fn) {
    const mp = (minPeriods === undefined) ? 1 : minPeriods
    const result = new Array(values.length)
    const acc = []
    for (let i = 0; i < values.length; i++) {
        if (isValid(values[i])) acc.push(values[i])
        result[i] = acc.length >= mp ? fn([...acc]) : NaN
    }
    return result
}

function aggSum(arr) { return arr.reduce((a, b) => a + b, 0) }
function aggMean(arr) { return arr.reduce((a, b) => a + b, 0) / arr.length }
function aggMin(arr) { return Math.min(...arr) }
function aggMax(arr) { return Math.max(...arr) }
function aggCount(arr) { return arr.length }

function aggStd(arr) {
    if (arr.length < 2) return NaN
    const m = aggMean(arr)
    const s2 = arr.reduce((a, v) => a + (v - m) * (v - m), 0)
    return Math.sqrt(s2 / (arr.length - 1))
}

function aggVar(arr) {
    if (arr.length < 2) return NaN
    const m = aggMean(arr)
    return arr.reduce((a, v) => a + (v - m) * (v - m), 0) / (arr.length - 1)
}

function aggMedian(arr) {
    const sorted = [...arr].sort((a, b) => a - b)
    const mid = Math.floor(sorted.length / 2)
    if (sorted.length % 2 === 0) return (sorted[mid - 1] + sorted[mid]) / 2
    return sorted[mid]
}

function Rolling(values, options) {
    const {window: windowSize, minPeriods, name, index} = options
    const mp = minPeriods

    function make(fn) {
        const result = windowAgg(values, windowSize, mp, fn)
        return Series(result, {name, index: [...index], dtype: 'float64'})
    }

    function aggSem(arr) {
        if (arr.length < 2) return NaN
        return aggStd(arr) / Math.sqrt(arr.length)
    }

    function aggSkew(arr) {
        const n = arr.length
        if (n < 3) return NaN
        const m = aggMean(arr)
        let m2 = 0, m3 = 0
        for (const v of arr) { const d = v - m; m2 += d * d; m3 += d * d * d }
        const sd = Math.sqrt(m2 / (n - 1))
        return (n / ((n - 1) * (n - 2))) * (m3 / (sd * sd * sd))
    }

    function aggKurt(arr) {
        const n = arr.length
        if (n < 4) return NaN
        const m = aggMean(arr)
        let m2 = 0, m4 = 0
        for (const v of arr) { const d = v - m; m2 += d * d; m4 += d * d * d * d }
        const variance = m2 / (n - 1)
        const num = (n * (n + 1)) / ((n - 1) * (n - 2) * (n - 3)) * (m4 / (variance * variance))
        return num - (3 * (n - 1) * (n - 1)) / ((n - 2) * (n - 3))
    }

    function aggQuantile(q) {
        return function(arr) {
            const sorted = [...arr].sort((a, b) => a - b)
            const pos = (sorted.length - 1) * q
            const lo = Math.floor(pos), hi = Math.ceil(pos)
            if (lo === hi) return sorted[lo]
            return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo)
        }
    }

    return {
        sum() { return make(aggSum) },
        mean() { return make(aggMean) },
        min() { return make(aggMin) },
        max() { return make(aggMax) },
        std() { return make(aggStd) },
        var() { return make(aggVar) },
        count() { return make(aggCount) },
        median() { return make(aggMedian) },
        apply(fn) { return make(fn) },
        quantile(q) { return make(aggQuantile(q)) },
        sem() { return make(aggSem) },
        skew() { return make(aggSkew) },
        kurt() { return make(aggKurt) }
    }
}

function Expanding(values, options) {
    const {minPeriods, name, index} = options
    const mp = minPeriods

    function make(fn) {
        const result = expandingAgg(values, mp, fn)
        return Series(result, {name, index: [...index], dtype: 'float64'})
    }

    return {
        sum() { return make(aggSum) },
        mean() { return make(aggMean) },
        min() { return make(aggMin) },
        max() { return make(aggMax) },
        std() { return make(aggStd) },
        var() { return make(aggVar) },
        count() { return make(aggCount) },
        apply(fn) { return make(fn) },
        quantile(q) {
            const qfn = function(arr) {
                const sorted = [...arr].sort((a, b) => a - b)
                const pos = (sorted.length - 1) * q
                const lo = Math.floor(pos), hi = Math.ceil(pos)
                if (lo === hi) return sorted[lo]
                return sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo)
            }
            return make(qfn)
        },
        sem() {
            const semFn = function(arr) {
                if (arr.length < 2) return NaN
                return aggStd(arr) / Math.sqrt(arr.length)
            }
            return make(semFn)
        }
    }
}

function Ewm(values, options) {
    const {span, name, index} = options
    const alpha = 2 / (span + 1)

    return {
        mean() {
            // pandas default: adjust=True
            // y_t = sum((1-alpha)^i * x_{t-i}) / sum((1-alpha)^i)
            const result = new Array(values.length)
            const validVals = []
            for (let i = 0; i < values.length; i++) {
                if (isValid(values[i])) {
                    validVals.push(values[i])
                    let num = 0, den = 0
                    for (let j = 0; j < validVals.length; j++) {
                        const w = Math.pow(1 - alpha, validVals.length - 1 - j)
                        num += w * validVals[j]
                        den += w
                    }
                    result[i] = num / den
                } else {
                    result[i] = NaN
                }
            }
            return Series(result, {name, index: [...index], dtype: 'float64'})
        }
    }
}

export { Rolling, Expanding, Ewm, windowAgg, expandingAgg }
export { aggSum, aggMean, aggMin, aggMax, aggStd, aggVar, aggCount, aggMedian }
