<template>
    <component
        :ref="iconId"
        :id="iconId"
        :class="{ hidden: hidden }"
        :is="icon"
    />
</template>

<script>
import { v4 as uuidv4 } from 'uuid';
import { defineAsyncComponent } from 'vue';
import assets from '@/utils/assets.js';

export default {
    name: 'SvgIcon',
    props: {
        name: {
            type: String,
            required: true,
        },
        hidden: {
            type: Boolean,
            default: false,
        },
    },
    data() {
        return {
            icons: assets.svgs,
        };
    },

    computed: {
        icon() {
            const name = this.name;
            // Dynamically import SVG icons as components
            return defineAsyncComponent(() =>
                this.icons[assets.pathToSvg(name)]()
            );
        },
        iconId() {
            return `svg-icon-${this.name}-${uuidv4()}`;
        },
    },
};
</script>

<style scoped>
.hidden {
    display: none;
}
</style>
