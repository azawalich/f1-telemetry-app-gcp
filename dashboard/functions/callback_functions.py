import dash_html_components as html

import data_assets.sections as sct
import functions.get_data as mdl_get_data
import modules.navigation as mdl_navigation
import modules.header as mdl_header
import modules.homepage as mdl_homepage
import modules.summary as mdl_summary

def return_dash_content(pathname, content_type, stats_data):
    pathname_clean = pathname
    sessionUID = None
    return_rendered_content = False
    message = "This page does not exist."
    if pathname != None:
        if len(pathname.split('%3F')) > 1:
            if pathname not in ['/', '/homepage']:
                pathname_clean, sessionUID = pathname.split('%3F')
                sessionUID = sessionUID.split('=')[1]
    
    if pathname_clean in ['/session-summary', '/session-laps'] and sessionUID != None:
        if sessionUID in stats_data['recent_statistics_df']['sessionUID'].tolist():
            return_rendered_content = True
    
    if content_type == 'navigation':
        if return_rendered_content:
            rendered_content = mdl_navigation.navigation_bar_links(stats = stats_data, sessionUID = sessionUID)
        else:
            rendered_content = mdl_navigation.navigation_bar(stats = stats_data['global_records'])
    elif content_type == 'header':
        if return_rendered_content:
            rendered_content = mdl_header.header_bar_session(stats = stats_data, pathname = pathname_clean, sessionUID = sessionUID)
        else:
            rendered_content = mdl_header.header_bar(stats = stats_data['global_statistics'], pathname = pathname_clean)
    elif content_type == 'page_content':
        if sessionUID != None:
            if sessionUID.isdigit():
                session_type = stats_data['recent_statistics_df'][stats_data['recent_statistics_df']['sessionUID'] == sessionUID]['sessionType'].tolist()[0]
            else:
                message = 'Wrong sessionUID - please don\'t mess-up with url GET parameter ;).'
        else:
            message = 'You can access this website after choosing session on a homepage first.'
        if pathname_clean in ["/", "/homepage"]:
            rendered_content = mdl_homepage.homepage_wrapper(stats = stats_data, page_size = 10)
        elif pathname_clean in ['/session-summary'] and sessionUID != None and sessionUID.isdigit():
            rendered_content = mdl_summary.summary_wrapper(sessionUID = sessionUID, session_type = session_type, page_size = 10)
        # elif pathname_clean in ['/session-laps'] and sessionUID != None and sessionUID.isdigit():
        #     rendered_content = html.Div(
        #         html.P("This is the content of page 3. Yay!"),
        #         id='page-content',
        #         style={'height': '690px'}
        #     )
        else:  
            # If the user tries to reach a different page, return a 404 message
            rendered_content = html.Div(
                [
                    html.H1("404: Not found", className="text-danger"),
                    html.P(message),
                    html.A('Back to homepage', href='/'),
                ],
                    id='page-content',
                    style={'height': '613px'}
                )
    return rendered_content

def return_active_class_elements(pathname, render_type):
    if pathname not in [None, '/', '/homepage']:
        pathname_clean, sessionUID = pathname.split('%3F')
        sessionUID = sessionUID.split('=')[1]
    else:
        pathname_clean = pathname

    if render_type == 'active_menu':
        css_class_name = 'navigation-link-current'
    elif render_type == 'active_menu_image':
        css_class_name = 'navigation-link-current-image'

    for single_indeks in range(1, len(sct.SECTIONS.keys())):
        single_section = list(sct.SECTIONS.keys())[single_indeks]
        if single_section == pathname_clean[1:]:
            exec('a{}="{}"'.format(single_indeks, css_class_name))
        else:
            exec('a{}=""'.format(single_indeks))

    return eval('a1'), eval('a2'), eval('a3'), eval('a4'), eval('a5'), eval('a6'), eval('a7')

def return_navigation_links(pathname):
    if pathname not in [None, '/', '/homepage']:
        sessionUID = pathname.split('%3F')[1].split('=')[1]
    
    for single_indeks in range(1, len(sct.SECTIONS.keys())):
        single_section = list(sct.SECTIONS.keys())[single_indeks]
        exec('a{}="/{}%3FsessionUID={}"'.format(single_indeks, single_section, sessionUID))
    return eval('a1'), eval('a2'), eval('a3'), eval('a4'), eval('a5'), eval('a6'), eval('a7')
