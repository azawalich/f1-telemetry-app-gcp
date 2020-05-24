import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import sys, os
import random 

sys.path.append(os.path.abspath(os.path.join('assets')))

import css_classes as css
import sections as sct

def create_tiles(data_dict):
    
    tile_elements = []
    
    for single_section in data_dict.keys():
        temp_section = data_dict[single_section]

        indeks = int(random.sample(range(1,10), k=1)[0])
        team_hex_color = css.TEAMS_COLORS['2020'][
                    list(
                        css.TEAMS_COLORS['2020'].keys()
                        )[indeks]
                        ]['hex']
        
        tile_css_updated = css.ENABLED_TILE_STYLE
        tile_css_updated['background-color'] = team_hex_color

        element = html.Div(
            [
                html.B(
                    temp_section,
                    style=css.ENABLED_LINK_TEXT_STYLE_BOLD
                    ),
                html.Br(),
                html.B(
                    single_section.replace('_', ' ').title(),
                    style=css.WHITE_COLOR
                )
            ], 
            style=tile_css_updated,
            className = 'm-4 p-auto',
            id='stats_tile_{}'.format(single_section),
        )

        tile_elements.append(element)
    
    return tile_elements