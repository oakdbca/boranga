<template lang="html">
    <div id="occurrenceCombineSites">
        <datatable
            :id="panelBody"
            ref="sites_datatable"
            :dt-options="sites_options"
            :dt-headers="sites_headers"
        />
    </div>
</template>

<script>
import { v4 as uuid } from 'uuid';
import datatable from '@vue-utils/datatable.vue';
import { constants, helpers } from '@/utils/hooks';

export default {
    name: 'OccurrenceCombineSites',
    components: {
        datatable,
    },
    props: {
        selectedSites: {
            type: Array,
            required: true,
        },
        combineSiteIds: {
            type: Array,
            required: true,
        },
        mainOccurrenceId: {
            type: Number,
            required: true,
        },
    },
    data: function () {
        let vm = this;
        return {
            panelBody: 'site-combine-select-' + uuid(),
            checkedSiteNames: [],
            sites_headers: [
                'Occurrence',
                'Number',
                'Name',
                'Coordinates',
                'Comments',
                'Related Occurrence Reports',
                'Action',
            ],
            sites_options: {
                autowidth: true,
                language: {
                    processing: constants.DATATABLE_PROCESSING_HTML,
                },
                paging: true,
                responsive: true,
                columnDefs: [
                    { responsivePriority: 1, targets: 0 },
                    { responsivePriority: 2, targets: -1 },
                ],
                data: vm.selectedSites,
                order: [],
                buttons: [],
                searching: true,
                dom:
                    "<'d-flex align-items-center'<'me-auto'l>fB>" +
                    "<'row'<'col-sm-12'tr>>" +
                    "<'d-flex align-items-center'<'me-auto'i>p>",
                columns: [
                    {
                        data: 'occurrence_number',
                        orderable: true,
                        searchable: true,
                    },
                    {
                        data: 'site_number',
                        orderable: true,
                        searchable: true,
                    },
                    {
                        data: 'site_name',
                        orderable: true,
                        searchable: true,
                    },
                    {
                        data: 'id',
                        mRender: function (data, type, full) {
                            if (full.point_coord1 && full.point_coord2) {
                                let coord1 = full.point_coord1.toString();
                                let coord2 = full.point_coord2.toString();

                                if (Number.isInteger(full.point_coord1)) {
                                    coord1 += '.0';
                                }
                                if (Number.isInteger(full.point_coord2)) {
                                    coord2 += '.0';
                                }

                                let value =
                                    coord1 +
                                    ', ' +
                                    coord2 +
                                    ' - ' +
                                    full.datum_name;
                                let result = helpers.dtPopover(
                                    value,
                                    30,
                                    'hover'
                                );
                                return result;
                            } else {
                                return '';
                            }
                        },
                    },
                    {
                        data: 'comments',
                        mRender: function (data, type, full) {
                            let value = full.comments;
                            let result = helpers.dtPopover(value, 30, 'hover');
                            return type == 'export' ? value : result;
                        },
                    },
                    {
                        data: 'related_occurrence_report_numbers',
                        mRender: function (data, type, full) {
                            let value = full.related_occurrence_report_numbers;
                            let result = helpers.dtPopover(value, 30, 'hover');
                            return result;
                        },
                    },
                    {
                        data: 'id',
                        mRender: function (data, type, full) {
                            if (vm.combineSiteIds.includes(full.id)) {
                                if (full.occurrence == vm.mainOccurrenceId) {
                                    return `<input id='${full.id}' data-site-checkbox='${full.id}' site-name='${full.site_name}' type='checkbox' checked disabled/>`;
                                } else {
                                    return `<input id='${full.id}' data-site-checkbox='${full.id}' site-name='${full.site_name}' type='checkbox' checked/>`;
                                }
                            } else {
                                if (
                                    vm.checkedSiteNames.includes(full.site_name)
                                ) {
                                    return `<input id='${full.id}' data-site-checkbox='${full.id}' site-name='${full.site_name}' type='checkbox' disabled/>`;
                                } else {
                                    return `<input id='${full.id}' data-site-checkbox='${full.id}' site-name='${full.site_name}' type='checkbox'/>`;
                                }
                            }
                        },
                    },
                ],
            },
            drawCallback: function () {
                setTimeout(function () {
                    vm.adjust_table_width();
                }, 100);
            },
            initComplete: function () {
                // another option to fix the responsive table overflow css on tab switch
                setTimeout(function () {
                    vm.adjust_table_width();
                }, 100);
            },
        };
    },
    created: function () {
        let vm = this;
        vm.getSelectedSiteNames();
    },
    mounted: function () {
        let vm = this;
        this.$nextTick(() => {
            vm.addEventListeners();
        });
    },
    methods: {
        getSelectedSiteNames: function () {
            let vm = this;
            let names = [];
            vm.selectedSites.forEach((site) => {
                if (
                    vm.combineSiteIds.includes(site.id) &&
                    !names.includes(site.site_name)
                ) {
                    names.push(site.site_name);
                }
            });
            vm.checkedSiteNames = names;
        },
        adjust_table_width: function () {
            if (this.$refs.sites_datatable !== undefined) {
                this.$refs.sites_datatable.vmDataTable.columns
                    .adjust()
                    .responsive.recalc();
            }
            helpers.enablePopovers();
        },
        removeSite: function (id) {
            let vm = this;
            vm.combineSiteIds.splice(vm.combineSiteIds.indexOf(id), 1);
            vm.getSelectedSiteNames();
        },
        addSite: function (id) {
            let vm = this;
            vm.combineSiteIds.push(id);
            vm.getSelectedSiteNames();
        },
        addEventListeners: function () {
            let vm = this;

            vm.$refs.sites_datatable.vmDataTable.on(
                'change',
                'input[data-site-checkbox]',
                function (e) {
                    e.preventDefault();
                    var id = parseInt($(this).attr('data-site-checkbox'));
                    if ($(this).prop('checked')) {
                        vm.addSite(id);
                        vm.selectedSites.forEach((site) => {
                            let checkbox =
                                vm.$refs.sites_datatable.vmDataTable.$(
                                    '#' + site.id
                                );
                            if (
                                id != checkbox.attr('data-site-checkbox') &&
                                checkbox.attr('site-name') ==
                                    $(this).attr('site-name')
                            ) {
                                checkbox.prop('disabled', true);
                            }
                        });
                    } else {
                        vm.removeSite(id);
                        vm.selectedSites.forEach((site) => {
                            let checkbox =
                                vm.$refs.sites_datatable.vmDataTable.$(
                                    '#' + site.id
                                );
                            if (
                                id != checkbox.attr('data-site-checkbox') &&
                                checkbox.attr('site-name') ==
                                    $(this).attr('site-name')
                            ) {
                                checkbox.prop('disabled', false);
                            }
                        });
                    }
                }
            );
            vm.$refs.sites_datatable.vmDataTable.on('draw', function () {
                helpers.enablePopovers();
            });
            vm.$refs.sites_datatable.vmDataTable.on('childRow.dt', function () {
                helpers.enablePopovers();
            });
        },
    },
};
</script>
