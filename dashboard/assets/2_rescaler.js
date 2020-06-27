function resize_dashboard(layout_width, window_width) {
    if (window_width > 1750){
    $("#scaleable-wrapper").css({
            transform: "scale(" + 0.8*(window_width / layout_width) + ")",  
            'transform-origin': 'top left'
        });
  } else {
    $("#scaleable-wrapper").attr('style', '');
    $("#layout-design").attr('style', '');
  }
  if (window_width > 1920){
    $("#sidebar").css({
        'height': $(window).height() * 0.65
    });
  }
  else {
    $("#sidebar").css({
        'height': $(window).height() * 0.89
    });
  }
};

var layout_width = $("#scaleable-wrapper").width();
var window_width = $(window).width();

$(window).on('load resize', function() {
    var dash_state = $('_dash-loading-callback')['prevObject'][0]['readyState']

    if (dash_state == 'complete'){
        setTimeout(function() {   
            var layout_width = $("#scaleable-wrapper").width();
            var window_width = $(window).width();
            resize_dashboard(layout_width, window_width);
        }, 100);
    }
    resize_dashboard(layout_width, window_width);
}).trigger('resize');

$(window).scroll(function() {
    // hacky solution to make scrolling work
    // $("#sidebar").css({
    //     "top": $(window).scrollTop(),
    // });

    // $("#header-top-row").css({
    //     "top": $(window).scrollTop()
    // });

    // $('#scroll-background').css({
    //     "top": $(window).scrollTop()
    // });
})