<template lang="html">
    <div class="container">
        <div class="row">
            <div class="col-sm-12">
                <div class="row">
                    <div
                        v-if="isOCRProposal"
                        class="col-sm-offset-3 col-sm-6 borderDecoration"
                    >
                        <div
                            v-if="
                                occurrence_report_obj.group_type ==
                                application_type_flora
                            "
                        >
                            <strong
                                >Your flora occurrence report has been
                                successfully submitted.</strong
                            >
                            <br />
                        </div>
                        <div
                            v-else-if="
                                occurrence_report_obj.group_type ==
                                application_type_fauna
                            "
                        >
                            <strong
                                >Your fauna occurrence report has been
                                successfully submitted.</strong
                            >
                            <br />
                        </div>
                        <div
                            v-else-if="
                                occurrence_report_obj.group_type ==
                                application_type_community
                            "
                        >
                            <strong
                                >Your community occurrence report has been
                                successfully submitted.</strong
                            >
                            <br />
                        </div>
                        <table>
                            <tbody>
                                <tr>
                                    <td><strong>Application:</strong></td>
                                    <td>
                                        <strong>{{
                                            occurrence_report_obj.occurrence_report_number
                                        }}</strong>
                                    </td>
                                </tr>
                                <tr>
                                    <td><strong>Date/Time:</strong></td>
                                    <td>
                                        <strong>
                                            {{
                                                formatDate(
                                                    occurrence_report_obj.lodgement_date
                                                )
                                            }}</strong
                                        >
                                    </td>
                                </tr>
                            </tbody>
                        </table>
                        <br />
                        <label
                            >You will receive a notification email if there is
                            any incomplete information or documents missing from
                            the proposal.</label
                        >
                        <router-link
                            :to="{ name: 'external-occurrence-report-dash' }"
                            style="margin-top: 15px"
                            class="btn btn-primary"
                            >Back to home</router-link
                        >
                    </div>
                    <div
                        v-else
                        class="col-sm-offset-3 col-sm-6 borderDecoration"
                    >
                        <strong
                            >Sorry it looks like there isn't any application
                            currently in your session.</strong
                        >
                        <br /><router-link
                            :to="{ name: 'external-occurrence-report-dash' }"
                            style="margin-top: 15px"
                            class="btn btn-primary"
                            >Back to home</router-link
                        >
                    </div>
                </div>
            </div>
        </div>
    </div>
</template>
<script>
import { api_endpoints } from '@/utils/hooks';

export default {
    beforeRouteEnter: function (to, from, next) {
        next((vm) => {
            vm.occurrence_report_obj = to.params.occurrence_report_obj;
            console.log(vm.occurrence_report_obj);
        });
    },
    data: function () {
        return {
            occurrence_report_obj: {},
        };
    },
    computed: {
        isOCRProposal: function () {
            return this.occurrence_report_obj && this.occurrence_report_obj.id
                ? true
                : false;
        },
        application_type_flora: function () {
            return api_endpoints.group_type_flora;
        },
        application_type_fauna: function () {
            return api_endpoints.group_type_fauna;
        },
        application_type_community: function () {
            return api_endpoints.group_type_community;
        },
    },
    mounted: function () {
        let vm = this;
        vm.form = document.forms.new_ocr_proposal;
    },
    methods: {
        formatDate: function (data) {
            return data ? moment(data).format('DD/MM/YYYY HH:mm:ss') : '';
        },
    },
};
</script>

<style lang="css" scoped>
.borderDecoration {
    border: 1px solid;
    border-radius: 5px;
    padding: 50px;
    margin-top: 70px;
}
</style>
