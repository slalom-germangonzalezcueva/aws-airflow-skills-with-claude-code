# ServiceNow incident defaults. Update `assignment_group` with the real ServiceNow assignment group.
DEFAULT_CONFIG = {
    "payload": {
        "category": "software",
        "impact": 6,
        "cmdb_ci": "MexAI App",
        "contact_type": "System Generated",
        "state": 1,
        "short_description": "",
        "description": "",
        "assignment_group": "TBD-SUPPORT-GROUP",
        "urgency_dict": {"C": 4, "H": 5, "M": 6, "L": 7},
        "urgency_name_mapping": {"C": "Critical", "H": "High", "M": "Medium", "L": "Low"},
    }
}
