import { Series, setDataFrame } from './series.js'
import { DataFrame } from './dataframe.js'
import { readCsv, toCsv, readJson, toJson, toNumeric, getDummies,
    read_csv, to_csv, read_json, to_json, to_numeric, get_dummies } from './io.js'
import { toDatetime, dateRange, to_datetime, date_range } from './datetime.js'
import { cut, qcut } from './utils.js'
import { run as _run, transpile } from './bridge.js'

// wire up circular dependency
setDataFrame(DataFrame)

function isna(value) {
    return value === null || value === undefined || (typeof value === 'number' && Number.isNaN(value))
}

function notna(value) {
    return !isna(value)
}

function concat(items, options) {
    const opts = options || {}
    const ignoreIndex = opts.ignoreIndex || false
    const axis = opts.axis || 0

    // axis=1: horizontal concat (column-wise)
    if (axis === 1) {
        // use index from first item, align by position
        const baseIndex = [...items[0].index]
        const resultData = {}
        const resultCols = []
        for (const df of items) {
            if (df._isPandasSeries) {
                const colName = df.name || resultCols.length
                resultData[colName] = [...df.values]
                resultCols.push(colName)
            } else {
                for (const c of df.columns) {
                    resultData[c] = [...df._colData(c)]
                    resultCols.push(c)
                }
            }
        }
        return DataFrame(resultData, {columns: resultCols, index: baseIndex})
    }

    // axis=0: vertical concat (row-wise)
    // all Series
    if (items[0]._isPandasSeries) {
        let vals = []
        let idxs = []
        for (const s of items) {
            vals = vals.concat(s.values)
            idxs = idxs.concat(s.index)
        }
        if (ignoreIndex) idxs = vals.map((_, i) => i)
        return Series(vals, {index: idxs})
    }
    // all DataFrames
    const allCols = new Set()
    for (const df of items) {
        for (const c of df.columns) allCols.add(c)
    }
    const cols = [...allCols]
    const resultData = {}
    for (const c of cols) resultData[c] = []
    let allIndex = []
    for (const df of items) {
        const n = df.shape[0]
        allIndex = allIndex.concat(df.index)
        for (const c of cols) {
            const cd = df._colData(c)
            if (cd) {
                for (const v of cd) resultData[c].push(v)
            } else {
                for (let i = 0; i < n; i++) resultData[c].push(null)
            }
        }
    }
    if (ignoreIndex) allIndex = resultData[cols[0]].map((_, i) => i)
    return DataFrame(resultData, {columns: cols, index: allIndex})
}

// top-level merge
function merge(left, right, options) {
    return left.merge(right, options)
}

// top-level unique
function unique(values) {
    const arr = values._isPandasSeries ? values.values : values
    return [...new Set(arr)]
}

// top-level factorize
function factorize(values) {
    const arr = values._isPandasSeries ? values.values : values
    const uniques = []
    const seen = {}
    const codes = arr.map(v => {
        if (seen[v] === undefined) { seen[v] = uniques.length; uniques.push(v) }
        return seen[v]
    })
    return [codes, uniques]
}

// top-level pivot_table
function pivotTable(df, options) {
    return df.pivotTable(options)
}

// top-level melt
function melt(df, options) {
    return df.melt(options)
}

// top-level crosstab
function crosstab(idx, col) {
    const idxVals = idx._isPandasSeries ? idx.values : idx
    const colVals = col._isPandasSeries ? col.values : col
    const rowKeys = [], colKeys = []
    const rowSeen = new Set(), colSeen = new Set()
    for (let i = 0; i < idxVals.length; i++) {
        if (!rowSeen.has(idxVals[i])) { rowKeys.push(idxVals[i]); rowSeen.add(idxVals[i]) }
        if (!colSeen.has(colVals[i])) { colKeys.push(colVals[i]); colSeen.add(colVals[i]) }
    }
    const resultData = {}
    for (const ck of colKeys) resultData[String(ck)] = new Array(rowKeys.length).fill(0)
    for (let i = 0; i < idxVals.length; i++) {
        const ri = rowKeys.indexOf(idxVals[i])
        resultData[String(colVals[i])][ri]++
    }
    return DataFrame(resultData, {columns: colKeys.map(String), index: rowKeys})
}

const pd = {
    Series, DataFrame, concat, cut, qcut, transpile,
    isna, notna, isnull: isna, notnull: notna,
    merge, unique, factorize, crosstab,
    pivotTable, pivot_table: pivotTable,
    melt,
    readCsv, toCsv, readJson, toJson, toNumeric, getDummies, toDatetime, dateRange,
    read_csv, to_csv, read_json, to_json, to_numeric, get_dummies, to_datetime, date_range,
}
// bind run so it always passes pd
pd.run = (pyCode) => _run(pyCode, pd)
export default pd
