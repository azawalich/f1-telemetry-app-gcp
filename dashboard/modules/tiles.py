import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import sys, os

sys.path.append(os.path.abspath(os.path.join('assets')))

import css_classes as css
import sections as sct

tile_elements = []

for single_section in sct.SECTIONS.keys():
    if single_section != 'homepage':
        temp_section = sct.SECTIONS[single_section]
        
        if temp_section['disabled']:
            tile_style = {
                'tile': css.DISABLED_TILE_STYLE,
                'image': css.DISABLED_COLOR
            }
        else:
            tile_style = {
                'tile': css.ENABLED_TILE_STYLE,
                'image': css.ENABLED_COLOR
            }
        
        element = html.A(
            [
                html.Img(
                    src=temp_section['icon'],
                    height="130px",
                    style=tile_style['image']
                    ),
                html.B(
                    temp_section['name'],
                    style=temp_section['link_style']
                    )
            ], 
            href="/{}".format(single_section),
            id="{}-link".format(single_section),
            style=tile_style['tile'],
            className = 'm-4 p-auto'
        )

        tile_elements.append(element)


def tile_bar():
    return tile_elements