// DateTime operations - toDatetime, dateRange, dt accessor
import { Series } from './series.js'

function toDatetime(data) {
    const values = data._isPandasSeries ? data.values : (Array.isArray(data) ? data : [data])
    const dates = values.map(v => {
        if (v === null || v === undefined) return null
        if (v instanceof Date) return v
        return new Date(v)
    })
    const name = data._isPandasSeries ? data.name : null
    const index = data._isPandasSeries ? [...data.index] : dates.map((_, i) => i)
    return Series(dates, {name, index, dtype: 'datetime64'})
}

function dateRange(options) {
    const {start, periods, freq = 'D', end} = options
    const dates = []
    const startDate = new Date(start)
    const count = periods || 0

    for (let i = 0; i < count; i++) {
        const d = new Date(startDate)
        if (freq === 'D') {
            d.setDate(d.getDate() + i)
        } else if (freq === 'H') {
            d.setHours(d.getHours() + i)
        } else if (freq === 'M') {
            d.setMonth(d.getMonth() + i)
        } else if (freq === 'Y') {
            d.setFullYear(d.getFullYear() + i)
        }
        dates.push(d)
    }
    return dates
}

function DtAccessor(values, options) {
    const {name, index} = options

    function mapDates(fn) {
        return Series(values.map(v => v === null ? null : fn(v)), {name, index: [...index]})
    }

    return {
        get year() { return mapDates(d => d.getFullYear()) },
        get month() { return mapDates(d => d.getMonth() + 1) },
        get day() { return mapDates(d => d.getDate()) },
        get hour() { return mapDates(d => d.getHours()) },
        get minute() { return mapDates(d => d.getMinutes()) },
        get second() { return mapDates(d => d.getSeconds()) },
        get dayofweek() { return mapDates(d => (d.getDay() + 6) % 7) },
        get dayofyear() {
            return mapDates(d => {
                const jan1 = new Date(d.getFullYear(), 0, 1)
                return Math.floor((d - jan1) / 86400000) + 1
            })
        },
        get quarter() { return mapDates(d => Math.floor(d.getMonth() / 3) + 1) },
        strftime(fmt) {
            return mapDates(d => {
                let result = fmt
                result = result.replace('%Y', String(d.getFullYear()))
                result = result.replace('%m', String(d.getMonth() + 1).padStart(2, '0'))
                result = result.replace('%d', String(d.getDate()).padStart(2, '0'))
                result = result.replace('%H', String(d.getHours()).padStart(2, '0'))
                result = result.replace('%M', String(d.getMinutes()).padStart(2, '0'))
                result = result.replace('%S', String(d.getSeconds()).padStart(2, '0'))
                return result
            })
        }
    }
}

// snake_case aliases for pandas compatibility
const to_datetime = toDatetime
const date_range = dateRange

export { toDatetime, dateRange, DtAccessor }
export { to_datetime, date_range }
