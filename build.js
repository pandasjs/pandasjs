import esbuild from 'esbuild'

// ESM bundle
await esbuild.build({
    entryPoints: ['src/index.js'],
    bundle: true,
    format: 'esm',
    outfile: 'dist/pandasjs.esm.js',
})

// CJS bundle
await esbuild.build({
    entryPoints: ['src/index.js'],
    bundle: true,
    format: 'cjs',
    outfile: 'dist/pandasjs.cjs',
})

// IIFE browser bundle (minified)
await esbuild.build({
    entryPoints: ['src/index.js'],
    bundle: true,
    format: 'iife',
    globalName: 'pd',
    minify: true,
    outfile: 'dist/pandasjs.min.js',
    footer: {
        // unwrap the default export so window.pd = the pd object directly
        js: 'pd = pd.default || pd;'
    },
})

console.log('built dist/pandasjs.esm.js, dist/pandasjs.cjs, dist/pandasjs.min.js')
