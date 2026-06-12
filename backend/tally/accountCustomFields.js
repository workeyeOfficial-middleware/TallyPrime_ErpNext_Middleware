export const ACCOUNT_CUSTOM_FIELDS = [

{
fieldname:"custom_tally_details",
label:"Tally Details",
fieldtype:"Section Break",
insert_after:"include_in_gross"
},

{
fieldname:"custom_under_group",
label:"Under Group",
fieldtype:"Data",
insert_after:"custom_tally_details"
},

{
fieldname:"custom_alias",
label:"Alias",
fieldtype:"Data",
insert_after:"custom_under_group"
},

{
fieldname:"custom_tally_col_1",
fieldtype:"Column Break",
insert_after:"custom_alias"
},

{
fieldname:"custom_ledger_type",
label:"Ledger Type",
fieldtype:"Data",
insert_after:"custom_tally_col_1"
},

{
fieldname:"custom_transaction_type",
label:"Transaction Type",
fieldtype:"Data",
insert_after:"custom_ledger_type"
},



{
fieldname:"custom_tax_section",
label:"Tax Registration Details",
fieldtype:"Section Break",
insert_after:"custom_transaction_type"
},

{
fieldname:"custom_gst_rate",
label:"GST Rate",
fieldtype:"Float",
insert_after:"custom_tax_section"
},

{
fieldname:"custom_hsn_sac",
label:"HSN SAC",
fieldtype:"Data",
insert_after:"custom_gst_rate"
},

{
fieldname:"custom_tax_col_1",
fieldtype:"Column Break",
insert_after:"custom_hsn_sac"
},

{
fieldname:"custom_pan_it_no",
label:"PAN / IT No",
fieldtype:"Data",
insert_after:"custom_tax_col_1"
},



{
fieldname:"custom_mailing_section",
label:"Mailing Details",
fieldtype:"Section Break",
insert_after:"custom_pan_it_no"
},

{
fieldname:"custom_mailing_name",
label:"Mailing Name",
fieldtype:"Data",
insert_after:"custom_mailing_section"
},

{
fieldname:"custom_mobile_no",
label:"Mobile No",
fieldtype:"Data",
insert_after:"custom_mailing_name"
},

{
fieldname:"custom_mail_col_1",
fieldtype:"Column Break",
insert_after:"custom_mobile_no"
},

{
fieldname:"custom_address",
label:"Address",
fieldtype:"Small Text",
insert_after:"custom_mail_col_1"
},



{
fieldname:"custom_bank_section",
label:"Banking Details",
fieldtype:"Section Break",
insert_after:"custom_address"
},

{
fieldname:"custom_provide_bank_details",
label:"Provide Bank Details",
fieldtype:"Check",
insert_after:"custom_bank_section"
},


// ADD THESE AT THE END OF THE ARRAY (before the closing ];)

{
  fieldname: "custom_opening_balance_section",
  label: "Opening Balance",
  fieldtype: "Section Break",
  insert_after: "custom_provide_bank_details"
},

{
  fieldname: "custom_tally_opening_balance",
  label: "Opening Balance",
  fieldtype: "Float",  // ← change Currency to Float
  insert_after: "custom_opening_balance_section"
},

{
  fieldname: "custom_tally_opening_balance_type",
  label: "Dr / Cr",
  fieldtype: "Select",
  options: "Dr\nCr",
  insert_after: "custom_tally_opening_balance"
},

{
  fieldname: "custom_ob_col_1",
  fieldtype: "Column Break",
  insert_after: "custom_tally_opening_balance_type"
},

{
  fieldname: "custom_tally_closing_balance",
  label: "Closing Balance",
  fieldtype: "Float",  // ← change Currency to Float
  insert_after: "custom_ob_col_1"
},

{
  fieldname: "custom_tally_closing_balance_type",
  label: "Dr / Cr",
  fieldtype: "Select",
  options: "Dr\nCr",
  insert_after: "custom_tally_closing_balance"
},

];