export default {
    svgs: import.meta.glob(`@assets/**/*.svg`, { query: '?component' }),
    svgs_raw: import.meta.glob(`@assets/**/*.svg`, { query: '?raw' }),
    svgs_url: import.meta.glob(`@assets/**/*.svg`, { query: '?url' }),
    /**
     * Returns the path to an asset
     * @param {String} name The name of the asset (e.g. 'my-icon.svg')
     * @returns {String|null} The path to the asset or null if not found
     */
    pathTo: function (name) {
        const [fileName, fileType] = name.split('.');
        const assets = this[`${fileType}s`];
        const keys = Object.keys(assets).filter((asset) =>
            asset.endsWith(`/${fileName}.${fileType}`)
        );
        if (keys.length === 0) {
            console.warn(`File not found: ${name}`);
            return null;
        } else if (keys.length > 1) {
            console.warn(`Multiple files found for: ${name}`);
        }

        return keys[0];
    },
    pathToSvg: function (name) {
        return this.pathTo(`${name}.svg`);
    },
    /**
     * Returns a promise of an asset import statement, e.g. import('/static/.../my-icon.svg')
     * @param {String} path The path to the asset
     * @param {String=} as The type of asset (e.g. 'component', 'raw', 'url')
     * @returns {Promise<String|null>} The asset import statement or null
     */
    asset: async function (path, as = 'component') {
        if (!path) {
            console.warn('No asset path provided');
            return null;
        }

        const fileType = path.split('.').pop();

        let _assets;
        if (as === 'raw') {
            _assets = this[`${fileType}s_raw`];
        } else if (as === 'component') {
            _assets = this[`${fileType}s`];
        } else if (as === 'url') {
            _assets = this[`${fileType}s_url`];
        } else {
            console.warn(`Unknown asset type: ${as}`);
            return null;
        }
        if (!_assets[path]) {
            console.warn(`Asset not found at path: ${path}`);
            return null;
        }

        console.log(
            `Fetching asset at path: ${path} as ${as}: ${_assets[path]}`
        );
        return await _assets[path]();
    },
};
