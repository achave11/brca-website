from django.conf.urls import url

from . import views

urlpatterns = [

    url(r'^$', views.index, name="index"),
    url(r'^suggestions/$', views.autocomplete),
    url(r'^ga4gh/variants/search$', views.index_num_2, name='index_num_2'),
    url(r'^ga4gh/variants/(?P<variant_id>.+)$', views.get_var_by_id, name = 'get_var_by_id'),
    url(r'^ga4gh/variantsets/search', views.get_variantSet, name='get_variantSet'),
    url(r'^ga4gh/variantsets/(?P<variantSetId>.+)$', views.get_varset_by_id, name='get_varset_by_id')
]
