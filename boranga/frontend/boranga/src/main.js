import 'vite/modulepreload-polyfill';

import { createApp } from 'vue';
import App from './App.vue';
import router from './router';
import helpers from '@/utils/helpers';
import VueSelect from 'vue-select';

import $ from 'jquery';
import select2 from 'select2';
window.$ = $;
import swal from 'sweetalert2';
window.swal = swal;
select2();

import 'datatables.net-bs5';
import 'datatables.net-buttons-bs5';
import 'datatables.net-responsive-bs5';
import 'datatables.net-buttons/js/dataTables.buttons.js';
import JSZip from 'jszip';
window.JSZip = JSZip;
import 'datatables.net-buttons/js/buttons.html5.js';
import 'select2';
import 'jquery-validation';

import 'sweetalert2/dist/sweetalert2.css';
import 'select2/dist/css/select2.min.css';
import 'select2-bootstrap-5-theme/dist/select2-bootstrap-5-theme.min.css';
import '@/../node_modules/datatables.net-bs5/css/dataTables.bootstrap5.min.css';
import '@/../node_modules/datatables.net-responsive-bs5/css/responsive.bootstrap5.min.css';
import '@/../node_modules/@fortawesome/fontawesome-free/css/all.min.css';

const app = createApp(App);

const fetch = window.fetch;
window.fetch = ((originalFetch) => {
    return async (...args) => {
        // Prepare headers
        let headers;
        if (args.length > 1) {
            headers =
                args[1].headers instanceof Headers
                    ? args[1].headers
                    : new Headers(args[1].headers || {});
        } else {
            headers = new Headers();
        }

        // Add CSRF token
        headers.set('X-CSRFToken', helpers.getCookie('csrftoken'));

        // Add Content-Type for JSON requests
        if (args.length > 1 && typeof args[1].body === 'string') {
            headers.set('Content-Type', 'application/json');
        }

        // Set headers back to args
        if (args.length > 1) {
            args[1].headers = headers;
        } else {
            args.push({ headers });
        }

        // Await the response to check status
        const response = await originalFetch.apply(this, args);

        // Redirect to login if 401 or 403 from /api endpoints (assume unauthenticated)
        if (
            response.status === 401 ||
            (response.status === 403 &&
                args[0] &&
                typeof args[0] === 'string' &&
                new URL(args[0], window.location.origin).pathname.startsWith(
                    '/api'
                ))
        ) {
            window.location.href =
                '/login/?next=' + encodeURIComponent(window.location.pathname);
        } else if (response.status === 403) {
            swal.fire({
                icon: 'error',
                title: 'Access Denied',
                text: 'You do not have permission to perform this action.',
                customClass: {
                    confirmButton: 'btn btn-primary',
                },
            });
        }

        return response;
    };
})(fetch);

app.component('v-select', VueSelect).use(router);
router.isReady().then(() => app.mount('#app'));
