// Utility functions - cut, qcut
import { Series } from './series.js'

function cut(data, bins, options) {
    const opts = options || {}
    const values = data._isPandasSeries ? data.values : data
    const labels = opts.labels
    const right = opts.right !== undefined ? opts.right : true

    // bins is a number: create equal-width bins
    let edges
    if (typeof bins === 'number') {
        const min = Math.min(...values.filter(v => v !== null && v !== undefined))
        const max = Math.max(...values.filter(v => v !== null && v !== undefined))
        const width = (max - min) / bins
        edges = []
        for (let i = 0; i <= bins; i++) {
            edges.push(min + i * width)
        }
        // extend first bin slightly to include min value
        edges[0] = edges[0] - 0.001
    } else {
        edges = bins
    }

    const result = values.map(v => {
        if (v === null || v === undefined || (typeof v === 'number' && isNaN(v))) return null
        for (let i = 0; i < edges.length - 1; i++) {
            const lo = edges[i]
            const hi = edges[i + 1]
            const inBin = right ? (v > lo && v <= hi) : (v >= lo && v < hi)
            if (inBin) {
                if (labels) return labels[i]
                if (right) return `(${lo}, ${hi}]`
                return `[${lo}, ${hi})`
            }
        }
        return null
    })

    if (data._isPandasSeries) {
        return Series(result, {name: data.name, index: [...data.index]})
    }
    return result
}

function qcut(data, q, options) {
    const opts = options || {}
    const values = data._isPandasSeries ? data.values : data
    const labels = opts.labels

    const sorted = values.filter(v => v !== null && v !== undefined).sort((a, b) => a - b)
    const n = sorted.length

    // compute quantile edges
    const edges = []
    for (let i = 0; i <= q; i++) {
        const pos = (n - 1) * (i / q)
        const lo = Math.floor(pos)
        const hi = Math.ceil(pos)
        if (lo === hi) {
            edges.push(sorted[lo])
        } else {
            edges.push(sorted[lo] + (sorted[hi] - sorted[lo]) * (pos - lo))
        }
    }
    // extend first edge slightly
    edges[0] = edges[0] - 0.001

    const result = values.map(v => {
        if (v === null || v === undefined || (typeof v === 'number' && isNaN(v))) return null
        for (let i = 0; i < edges.length - 1; i++) {
            if (v > edges[i] && v <= edges[i + 1]) {
                if (labels) return labels[i]
                return `(${edges[i]}, ${edges[i + 1]}]`
            }
        }
        return null
    })

    if (data._isPandasSeries) {
        return Series(result, {name: data.name, index: [...data.index]})
    }
    return result
}

export { cut, qcut }
