import dash_html_components as html

import data_assets.sections as sct
import modules.get_data as mdl_get_data
import modules.navigation as mdl_navigation
import modules.header as mdl_header
import modules.homepage as mdl_homepage

OG_META_TAGS = [
        {
            'property': 'og:title',
            'content': 'F1 Telemetry App - Aleksander Zawalich'
        },
        {
            'property': 'og:description',
            'content': 'F1 telemetry app hosted on GCP'
        },
        {
            'property': 'og:image',
            'content': 'https://www.codemasters.com/wp-content/uploads/2019/03/f1_2019_monza_010.jpg'
        },
        {
            'property': 'og:url',
            'content': 'http://f1.zawalich.pl'
        },
        {
            'property': 'og:type',
            'content': 'website'
        },
        {
            'http-equiv': 'X-UA-Compatible',
            'content': 'IE=edge'
        },
        {
            'charset': 'UTF-8'
        },
    ]

def return_dash_content(pathname, content_type, stats_data):
    pathname_clean = pathname
    sessionUID = None
    return_rendered_content = False

    if pathname != None:
        if len(pathname.split('%3F')) > 1:
            if pathname not in ['/', '/homepage']:
                pathname_clean, sessionUID = pathname.split('%3F')
                sessionUID = sessionUID.split('=')[1]
    
    if pathname_clean in ['/session-summary'] and sessionUID != None:
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
        if pathname_clean in ["/", "/homepage"]:
            rendered_content = mdl_homepage.homepage_wrapper(stats = stats_data, page_size = 10)
        elif return_rendered_content:
                rendered_content = html.Div(
                    html.P("This is the content of page 2. Yay!"),
                    id='page-content',
                    style={'height': '730px'}
                )
        else:  
            # If the user tries to reach a different page, return a 404 message
            rendered_content = html.Div(
                [
                    html.H1("404: Not found", className="text-danger"),
                    html.P(f"The pathname {pathname_clean} was not recognised..."),
                    html.A('Back to homepage', href='/'),
                ],
                    id='page-content',
                    style={'height': '613px'}
                )
    return rendered_content

def return_active_class_elements(pathname, render_type):
    if pathname not in [None, '/', '/homepage']:
            pathname_clean = pathname.split('%3F')[0]
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
