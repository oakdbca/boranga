<template lang="html">
    <div id="species-combine-threats">
        <FormSection
            :form-collapse="false"
            label="Threats"
            :Index="threatFormSectionIndex"
        >
            <form class="form-horizontal" method="post">
                <div class="col-sm-12 form-check form-check-inline">
                    <input
                        :id="'threat_select_all' + combine_species.id"
                        class="form-check-input"
                        type="radio"
                        :name="'threatSelect-' + combine_species.id"
                        value="selectAll"
                        :checked="currentThreatSelection === 'selectAll'"
                        @change="selectThreatOption"
                    />
                    <label class="form-check-label">
                        Copy all threats from Species
                        {{ combine_species.species_number }}
                    </label>
                </div>
                <div class="col-sm-12 form-check form-check-inline mb-3">
                    <input
                        :id="'threat_select_individual' + combine_species.id"
                        class="form-check-input"
                        type="radio"
                        :name="'threatSelect-' + combine_species.id"
                        value="individual"
                        :checked="currentThreatSelection === 'individual'"
                        @change="selectThreatOption"
                    />
                    <label class="form-check-label">Decide per threat</label>
                </div>
                <div>
                    <datatable
                        :id="threatsDatatableId"
                        ref="threats_datatable"
                        :dt-options="threats_options"
                        :dt-headers="threats_headers"
                    />
                </div>
            </form>
        </FormSection>
    </div>
</template>
<script>
import { v4 as uuid } from 'uuid';
import datatable from '@vue-utils/datatable.vue';
import FormSection from '@/components/forms/section_toggle.vue';
import { constants, api_endpoints, helpers } from '@/utils/hooks';
import moment from 'moment';

export default {
    name: 'SpeciesCombineThreats',
    components: { FormSection, datatable },
    props: {
        resulting_species_community: { type: Object, required: true },
        combine_species: { type: Object, required: true },
    },
    data() {
        const vm = this;
        return {
            threatFormSectionIndex: 'threat-form-section-index-' + uuid(),
            threatsDatatableId: 'threats-datatable-id-' + uuid(),
            // All threat ids for this species (fetched once)
            combine_species_threat_ids: [],
            threats_headers: [
                'Number',
                'Category',
                'Threat Source',
                'Date Observed',
                'Threat Agent',
                'Comments',
                'Current Impact?',
                'Potential Impact?',
                'Action',
            ],
            threats_options: {
                autowidth: false,
                language: { processing: constants.DATATABLE_PROCESSING_HTML },
                responsive: true,
                searching: true,
                columnDefs: [
                    { responsivePriority: 1, targets: 0 },
                    { responsivePriority: 2, targets: -1 },
                ],
                ajax: {
                    url: helpers.add_endpoint_json(
                        api_endpoints.species,
                        vm.combine_species.id + '/threats'
                    ),
                    dataSrc: '',
                },
                order: [[0, 'desc']],
                dom:
                    "<'d-flex align-items-center'<'me-auto'l>fB>" +
                    "<'row'<'col-sm-12'tr>>" +
                    "<'d-flex align-items-center'<'me-auto'i>p>",
                buttons: [
                    {
                        extend: 'excel',
                        title: 'Boranga Species Combine Threats Excel Export',
                        text: '<i class="fa-solid fa-download"></i> Excel',
                        className: 'btn btn-primary me-2 rounded',
                        exportOptions: { orthogonal: 'export' },
                    },
                    {
                        extend: 'csv',
                        title: 'Boranga Species Combine Threats CSV Export',
                        text: '<i class="fa-solid fa-download"></i> CSV',
                        className: 'btn btn-primary rounded',
                        exportOptions: { orthogonal: 'export' },
                    },
                ],
                columns: [
                    {
                        data: 'threat_number',
                        orderable: true,
                        searchable: true,
                        mRender(data, type, full) {
                            return full.visible
                                ? full.threat_number
                                : '<s>' + full.threat_number + '</s>';
                        },
                    },
                    {
                        data: 'threat_category',
                        orderable: true,
                        searchable: true,
                        mRender(data, type, full) {
                            return full.visible
                                ? full.threat_category
                                : '<s>' + full.threat_category + '</s>';
                        },
                    },
                    {
                        data: 'source',
                        orderable: true,
                        searchable: true,
                        mRender(data, type, full) {
                            return full.visible
                                ? full.source
                                : '<s>' + full.source + '</s>';
                        },
                    },
                    {
                        data: 'date_observed',
                        mRender(data, type, full) {
                            if (!data) return '';
                            const formatted = moment(data).format('DD/MM/YYYY');
                            return full.visible
                                ? formatted
                                : '<s>' + formatted + '</s>';
                        },
                    },
                    {
                        data: 'threat_agent',
                        orderable: true,
                        searchable: true,
                        mRender(data, type, full) {
                            return full.visible
                                ? full.threat_agent
                                : '<s>' + full.threat_agent + '</s>';
                        },
                    },
                    {
                        data: 'comment',
                        orderable: true,
                        searchable: true,
                        render(value, type, full) {
                            const result = helpers.dtPopover(
                                value,
                                30,
                                'hover'
                            );
                            if (full.visible) {
                                return type === 'export' ? value : result;
                            }
                            return type === 'export'
                                ? value
                                : '<s>' + result + '</s>';
                        },
                    },
                    {
                        data: 'current_impact_name',
                        orderable: true,
                        searchable: true,
                        mRender(data, type, full) {
                            return full.visible
                                ? full.current_impact_name
                                : '<s>' + full.current_impact_name + '</s>';
                        },
                    },
                    {
                        data: 'potential_impact_name',
                        orderable: true,
                        searchable: true,
                        mRender(data, type, full) {
                            return full.visible
                                ? full.potential_impact_name
                                : '<s>' + full.potential_impact_name + '</s>';
                        },
                    },
                    {
                        data: 'id',
                        mRender(data, type, full) {
                            const isSelectAll =
                                vm.isThreatSelectAll ||
                                vm.resulting_species_community
                                    .copy_all_threats === true;
                            const ids =
                                vm.resulting_species_community
                                    .threat_ids_to_copy || [];
                            const isChecked =
                                isSelectAll || ids.includes(full.id);
                            const isDisabled = isSelectAll;
                            return `<input class='form-check-input' type="checkbox" data-add-threat="${full.id}"${
                                isChecked ? ' checked' : ''
                            }${isDisabled ? ' disabled' : ''}>`;
                        },
                    },
                ],
                processing: true,
                drawCallback() {
                    helpers.enablePopovers();
                },
                initComplete: (settings, json) => {
                    helpers.enablePopovers();
                    setTimeout(() => this.adjust_table_width(), 100);
                    if (json && this.combine_species_threat_ids.length === 0) {
                        this.combine_species_threat_ids = json.map((d) => d.id);
                    }
                    if (!this.resulting_species_community.threat_ids_to_copy) {
                        this.resulting_species_community.threat_ids_to_copy =
                            [];
                    }
                    if (this.isThreatSelectAll) {
                        this.unionThreats(this.combine_species_threat_ids);
                        this.resulting_species_community.copy_all_threats = true;
                    }
                },
            },
        };
    },
    mounted() {
        // Default to selectAll per-species (stop using a shared parent threat_selection)
        if (
            !this.combine_species.threat_selection &&
            !this.resulting_species_community.copy_all_threats
        ) {
            this.combine_species.threat_selection = 'selectAll';
            this.resulting_species_community.copy_all_threats = true;
        }
        this.addEventListeners();
    },
    beforeUnmount() {
        const dt =
            this.$refs.threats_datatable &&
            this.$refs.threats_datatable.vmDataTable;
        if (dt) {
            dt.off('change', 'input[data-add-threat]');
            dt.off('childRow.dt');
        }
    },
    computed: {
        currentThreatSelection() {
            if (this.combine_species.threat_selection) {
                return this.combine_species.threat_selection;
            }
            return this.resulting_species_community.copy_all_threats
                ? 'selectAll'
                : 'individual';
        },
        isThreatSelectAll() {
            return this.currentThreatSelection === 'selectAll';
        },
    },
    methods: {
        selectThreatOption(e) {
            const selected = e.target.value;
            if (this.currentThreatSelection === selected) return;

            // Update only this species (removed parent shared mutation)
            this.combine_species.threat_selection = selected;

            if (selected === 'selectAll') {
                this.unionThreats(this.combine_species_threat_ids);
                this.resulting_species_community.copy_all_threats = true;
            } else {
                this.resulting_species_community.copy_all_threats = false;
            }

            this.$nextTick(() =>
                this.$refs.threats_datatable.vmDataTable.ajax.reload(
                    null,
                    false
                )
            );
        },
        unionThreats(ids) {
            const global =
                this.resulting_species_community.threat_ids_to_copy || [];
            const set = new Set(global);
            ids.forEach((id) => set.add(id));
            this.resulting_species_community.threat_ids_to_copy = [...set];
        },
        addEventListeners() {
            const dt =
                this.$refs.threats_datatable &&
                this.$refs.threats_datatable.vmDataTable;
            if (!dt) return;
            dt.on('change', 'input[data-add-threat]', (evt) => {
                if (this.isThreatSelectAll) return;
                const id = parseInt(
                    evt.currentTarget.getAttribute('data-add-threat')
                );
                if (Number.isNaN(id)) return;
                const list =
                    this.resulting_species_community.threat_ids_to_copy;
                const idx = list.indexOf(id);
                if (evt.currentTarget.checked) {
                    if (idx === -1) list.push(id);
                } else if (idx > -1) {
                    list.splice(idx, 1);
                }
                this.resulting_species_community.threat_ids_to_copy =
                    list.slice();
            });
            dt.on('childRow.dt', () => helpers.enablePopovers());
        },
        adjust_table_width() {
            this.$nextTick(() => {
                if (this.$refs.threats_datatable) {
                    this.$refs.threats_datatable.vmDataTable.columns
                        .adjust()
                        .responsive.recalc();
                }
            });
        },
    },
};
</script>
