def format_yarn_application_overview(application_page_info = ""):
    import re, sys
    import prettytable
    pt = prettytable.PrettyTable()
    
    def resolve_yarn_app_id(args = []):
        appIdLines = filter(lambda _key: (_key.find("application_") >= 0 ),args)
        if(len(appIdLines) > 0) :
            appIdLine = appIdLines[0]
            YARN_REGEX_PATTERN="""application_(\d+)_(\d+)"""
            re_search_rst = re.compile(YARN_REGEX_PATTERN).search(appIdLine)
            if(re_search_rst != None):
                appId = re.compile(YARN_REGEX_PATTERN).search(appIdLine).group(0)
                return appId
            else:
                return "NULL_YARN_APPLICATION_ID"
        else:
            return "NULL_YARN_APPLICATION_ID"
    
    # application_page_info = sys.argv[1]
    application_overview = application_page_info.split("Application Overview")[1]
    appId = resolve_yarn_app_id([application_page_info])
    appName = "Application"
    pt.field_names = [appName, appId]
    rows = [ line.split(":\t") for line in application_overview.split('\n') if len(line) > 0]
    for r in rows:
       pt.add_row(r) 
    
    print pt

def format_weiflow_submit_overview(weiflow_submit_overview = ""):
    import prettytable
    pt = prettytable.PrettyTable()
    
    pt.field_names = ["Keys", "Values"]
    command_parameters = ''.join([line for line in weiflow_submit_overview.split('\n') if '--' in line])
    parameters_kv = [kv_line.strip() for kv_line in command_parameters.split('--') if len(kv_line.strip()) > 0 ]
    row_kv_list = [kv_line.strip() for kv_line in parameters_kv ]
    rows = map(lambda kv : kv.split(' ') , row_kv_list)
    for r in rows:
       pt.add_row(r) 
    
    print pt
