from django.conf.urls import url

from . import views

urlpatterns = [

    url(r'^$', views.index, name="index"),
    url(r'^suggestions/$', views.autocomplete),
    url(r'^ga4gh/$', views.index_num_2, name='index_num_2')
]
