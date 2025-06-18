<template>
    <div class="card card-default">
        <div class="card-header">Actviation Details</div>
        <div class="card-body py-2">
            <strong>Activated by</strong><br />
            {{ submitter_first_name }}
            {{ submitter_last_name }}
        </div>
        <div v-if="lodgement_date" class="card-body border-top py-2">
            <strong>Activated on</strong><br />
            {{ formatDateDay(lodgement_date) }}
            <span class="fw-light">at</span>
            {{ formatDateTime(lodgement_date) }}
        </div>
    </div>
</template>

<script>
export default {
    name: 'ActivatedByComponent',
    props: {
        submitter_first_name: {
            type: String,
            default: '',
        },
        submitter_last_name: {
            type: String,
            default: '',
        },
        lodgement_date: {
            type: String,
            default: null,
        },
    },
    methods: {
        formatDateDay: function (data) {
            return data ? moment(data).format('DD/MM/YYYY') : '';
        },
        formatDateTime: function (data) {
            return data ? moment(data).format('HH:mm:ss A') : '';
        },
    },
};
</script>
