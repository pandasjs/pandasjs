# PandasJS

Vanilla JS DataFrame library inspired by Pandas API. Zero dependencies, 503 tests verified against Python pandas.

## Performance

Measured on small test data. Results vary by workload and environment.

| Area                    | Python  | JS      | Ratio |
| ----------------------- | ------- | ------- | ----- |
| Creation & Access       | 0.72 ms | 0.83 ms | 0.9x  |
| Selection & Indexing    | 1.68 ms | 1.08 ms | 1.6x  |
| Computation             | 2.91 ms | 1.34 ms | 2.2x  |
| Reshaping               | 5.40 ms | 2.43 ms | 2.2x  |
| Advanced                | 5.70 ms | 2.05 ms | 2.8x  |
| Window                  | 2.12 ms | 1.93 ms | 1.1x  |
| GroupBy & Merge         | 7.83 ms | 2.05 ms | 3.8x  |
| IO & Data Types         | 4.42 ms | 1.79 ms | 2.5x  |
| DateTime & Strings      | 3.64 ms | 2.21 ms | 1.6x  |
| Utilities               | 4.24 ms | 1.82 ms | 2.3x  |
| Arithmetic & Conversions| 2.31 ms | 2.04 ms | 1.1x  |
| Cumulative & Stats      | 2.81 ms | 2.05 ms | 1.4x  |
| GroupBy Enhancements    | 2.03 ms | 1.18 ms | 1.7x  |
| Reshape & Top-level     | 5.34 ms | 2.03 ms | 2.6x  |
| String & Window Ext     | 1.98 ms | 1.64 ms | 1.2x  |

JS is faster in most areas (1.1-3.8x), comparable in creation and arithmetic (~0.9x).

## Structure
```
src/
    series.js      Series(data, options) factory
    dataframe.js   DataFrame(data, options) factory
    window.js      Rolling/Expanding/Ewm window factories
    groupby.js     GroupBy factory (multi-col, agg, transform, filter, cumulative)
    io.js          readCsv/toCsv/readJson/toJson/getDummies/toNumeric
    datetime.js    toDatetime/dateRange/DtAccessor
    str.js         StrAccessor (extended string operations)
    utils.js       cut/qcut binning utilities
    bridge.js      pd.run() Python→JS transpiler, pd.transpile()
    index.js       default export pd = { Series, DataFrame, merge, crosstab, ... }
dist/
    pandasjs.esm.js   ES module bundle
    pandasjs.cjs      CommonJS bundle
    pandasjs.min.js   IIFE browser bundle (window.pd)
docs/
    index.html     Landing page (hero, features, install, perf)
    manual.html    Documentation with side-by-side Python/JS examples
    manual.js      Pyodide runner + JS code execution for docs
    style.css      Shared CSS (dark theme, shine effects)
test/
    runner.js      CLI comparison runner (py vs js JSON diff + perf)
    stage1/        Creation & Access tests ✓ 26/26
    stage2/        Selection & Indexing tests ✓ 32/32
    stage3/        Computation & Aggregation tests ✓ 39/39
    stage4/        Reshaping & Combining tests ✓ 49/49
    stage5/        Advanced tests ✓ 45/45
    stage6/        Window & Aggregation tests ✓ 43/43
    stage7/        GroupBy & Merge tests ✓ 35/35
    stage8/        IO & Data Types tests ✓ 37/37
    stage9/        DateTime & String tests ✓ 33/33
    stage10/       Utilities tests ✓ 33/33
    stage11/       Arithmetic, Conversions & Aliases ✓ 51/51
    stage12/       DataFrame Cumulative & Stats ✓ 23/23
    stage13/       GroupBy Enhancements ✓ 11/11
    stage14/       Reshape Extensions & Top-level ✓ 24/24
    stage15/       String & Window Extensions ✓ 22/22
```

## API Status

| Stage | Area                             | Status     | Tests |
| ----- | -------------------------------- | ---------- | ----- |
| 1     | Creation & Access                | ✓ complete | 26/26 |
| 2     | Selection & Indexing             | ✓ complete | 32/32 |
| 3     | Computation & Aggregation        | ✓ complete | 39/39 |
| 4     | Reshaping & Combining            | ✓ complete | 49/49 |
| 5     | Advanced                         | ✓ complete | 45/45 |
| 6     | Window & Aggregation             | ✓ complete | 43/43 |
| 7     | GroupBy & Merge                  | ✓ complete | 35/35 |
| 8     | IO & Data Types                  | ✓ complete | 37/37 |
| 9     | DateTime & Strings               | ✓ complete | 33/33 |
| 10    | Utilities                        | ✓ complete | 33/33 |
| 11    | Arithmetic, Conversions, Aliases | ✓ complete | 51/51 |
| 12    | DataFrame Cumulative & Stats     | ✓ complete | 23/23 |
| 13    | GroupBy Enhancements             | ✓ complete | 11/11 |
| 14    | Reshape Extensions & Top-level   | ✓ complete | 24/24 |
| 15    | String & Window Extensions       | ✓ complete | 22/22 |

## Design
- Factory functions return plain objects with methods
- `import pd from './src/index.js'` then `pd.Series(...)`, `pd.DataFrame(...)`
- No dependencies, no class/prototype
- Tests output JSON `{tests: [{name, result}], elapsed_ms}`, runner diffs py vs js

## Publishing
- npm: `pandasjs` (ESM, CJS, IIFE browser bundle)
- CDN: `https://cdn.jsdelivr.net/npm/@rockiey/pandasjs/dist/pandasjs.min.js`
- Build: `node build.js` (esbuild, 3 outputs)
- pd.run(): transpiles Python pandas code to JS and runs it natively (no Pyodide)
- Docs: hosted at pandasjs.github.io (GitHub Pages from docs/ folder)

