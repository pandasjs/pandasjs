// IO operations - CSV/JSON read/write, getDummies, toNumeric
import { Series } from './series.js'
import { DataFrame } from './dataframe.js'

function readCsv(csvString, options) {
    const opts = options || {}
    const sep = opts.sep || ','
    const header = opts.header !== undefined ? opts.header : 0
    const indexCol = opts.indexCol
    const skipRows = opts.skipRows || 0
    const nRows = opts.nRows

    const lines = csvString.trim().split('\n')
    const startLine = skipRows
    let columns = null
    let dataStart = startLine

    if (header !== null) {
        columns = lines[startLine + header].split(sep).map(s => s.trim())
        dataStart = startLine + header + 1
    }

    let endLine = lines.length
    if (nRows !== undefined) endLine = Math.min(dataStart + nRows, lines.length)

    const rows = []
    for (let i = dataStart; i < endLine; i++) {
        if (lines[i].trim() === '') continue
        rows.push(lines[i].split(sep).map(s => s.trim()))
    }

    if (columns === null) {
        columns = rows[0].map((_, i) => i)
    }

    const colData = {}
    for (let ci = 0; ci < columns.length; ci++) {
        colData[columns[ci]] = rows.map(row => {
            const val = row[ci]
            if (val === '' || val === undefined) return null
            const num = Number(val)
            if (!isNaN(num) && val !== '') return num
            if (val === 'true' || val === 'True') return true
            if (val === 'false' || val === 'False') return false
            return val
        })
    }

    let idx = undefined
    let finalCols = [...columns]
    if (indexCol !== undefined) {
        const icName = typeof indexCol === 'number' ? columns[indexCol] : indexCol
        idx = colData[icName]
        finalCols = columns.filter(c => c !== icName)
        const filteredData = {}
        for (const c of finalCols) filteredData[c] = colData[c]
        return DataFrame(filteredData, {columns: finalCols, index: idx})
    }

    return DataFrame(colData, {columns: finalCols})
}

function toCsv(df, options) {
    const opts = options || {}
    const sep = opts.sep || ','
    const includeIndex = opts.index !== undefined ? opts.index : true
    const includeHeader = opts.header !== undefined ? opts.header : true

    const lines = []
    if (includeHeader) {
        const headerParts = includeIndex ? [''] : []
        headerParts.push(...df.columns)
        lines.push(headerParts.join(sep))
    }
    const values = df.values
    for (let i = 0; i < values.length; i++) {
        const parts = includeIndex ? [df.index[i]] : []
        parts.push(...values[i].map(v => v === null || v === undefined ? '' : v))
        lines.push(parts.join(sep))
    }
    return lines.join('\n') + '\n'
}

function readJson(jsonString) {
    const data = JSON.parse(jsonString)
    // array of records
    if (Array.isArray(data)) {
        return DataFrame(data)
    }
    // dict of columns: {col: [values]}
    return DataFrame(data)
}

function toJson(df, options) {
    const opts = options || {}
    const orient = opts.orient || 'records'
    if (orient === 'records') {
        return JSON.stringify(df.toJSON())
    }
    if (orient === 'columns') {
        const result = {}
        for (const c of df.columns) {
            const col = df._colData(c)
            const indexed = {}
            for (let i = 0; i < col.length; i++) {
                indexed[String(df.index[i])] = col[i]
            }
            result[c] = indexed
        }
        return JSON.stringify(result)
    }
    if (orient === 'index') {
        const result = {}
        const values = df.values
        for (let i = 0; i < df.index.length; i++) {
            const row = {}
            for (let j = 0; j < df.columns.length; j++) {
                row[df.columns[j]] = values[i][j]
            }
            result[df.index[i]] = row
        }
        return JSON.stringify(result)
    }
    return JSON.stringify(df.toJSON())
}

function toNumeric(data, options) {
    const opts = options || {}
    const errors = opts.errors || 'raise'
    const values = data._isPandasSeries ? data.values : (Array.isArray(data) ? data : [data])
    const result = values.map(v => {
        if (v === null || v === undefined) return NaN
        const num = Number(v)
        if (isNaN(num)) {
            if (errors === 'coerce') return NaN
            return v
        }
        return num
    })
    if (data._isPandasSeries) {
        return Series(result, {name: data.name, index: [...data.index], dtype: 'float64'})
    }
    return result
}

function getDummies(data, options) {
    const opts = options || {}
    const dropFirst = opts.dropFirst || false
    const prefix = opts.prefix

    if (data._isPandasSeries) {
        const vals = data.values
        const categories = [...new Set(vals)].sort()
        const cats = dropFirst ? categories.slice(1) : categories
        const resultData = {}
        for (const cat of cats) {
            const colName = prefix ? prefix + '_' + cat : cat
            resultData[String(colName)] = vals.map(v => v === cat)
        }
        return DataFrame(resultData, {columns: cats.map(c => String(prefix ? prefix + '_' + c : c)), index: data.index ? [...data.index] : undefined})
    }

    // DataFrame
    if (data._isPandasDataFrame) {
        const targetCols = opts.columns || data.columns.filter(c => {
            const first = data._colData(c).find(v => v !== null && v !== undefined)
            return typeof first === 'string'
        })

        const resultData = {}
        const resultCols = []

        for (const c of data.columns) {
            if (targetCols.includes(c)) {
                const vals = data._colData(c)
                const categories = [...new Set(vals)].sort()
                const cats = dropFirst ? categories.slice(1) : categories
                for (const cat of cats) {
                    const colName = c + '_' + cat
                    resultData[colName] = vals.map(v => v === cat)
                    resultCols.push(colName)
                }
            } else {
                resultData[c] = [...data._colData(c)]
                resultCols.push(c)
            }
        }
        return DataFrame(resultData, {columns: resultCols, index: [...data.index]})
    }
}

// snake_case aliases for pandas compatibility
const read_csv = readCsv
const to_csv = toCsv
const read_json = readJson
const to_json = toJson
const to_numeric = toNumeric
const get_dummies = getDummies

export { readCsv, toCsv, readJson, toJson, toNumeric, getDummies }
export { read_csv, to_csv, read_json, to_json, to_numeric, get_dummies }
