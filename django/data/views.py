import os
import re
import tempfile
from operator import __or__

from django.db import connection
from django.db.models import Q
from django.http import JsonResponse, HttpResponse
from django.views.decorators.gzip import gzip_page

from .models import Variant
#########################################################
from django.views.decorators.http import require_http_methods
from ga4gh import variant_service_pb2 as v_s
from ga4gh import variants_pb2 as vrs
import google.protobuf.json_format as json_format
from django.core.exceptions import SuspiciousOperation
#import ga4gh.client as client
#########################################################

@gzip_page
def index(request):
    order_by = request.GET.get('order_by')
    direction = request.GET.get('direction')
    page_size = int(request.GET.get('page_size', '0'))
    page_num = int(request.GET.get('page_num', '0'))
    search_term = request.GET.get('search_term')
    format = request.GET.get('format')
    include = request.GET.getlist('include')
    exclude = request.GET.getlist('exclude')
    filters = request.GET.getlist('filter')
    filter_values = request.GET.getlist('filterValue')
    column = request.GET.getlist('column')

    query = Variant.objects

    if format == 'csv':
        quotes = '\''
    else:
        quotes = ''

    if include or exclude:
        query = apply_sources(query, include, exclude)

    if filters:
        query = apply_filters(query, filter_values, filters, quotes=quotes)

    if search_term:
        query = apply_search(query, search_term, quotes=quotes)

    if order_by:
        query = apply_order(query, order_by, direction)

    if format == 'csv':

        cursor = connection.cursor()
        with tempfile.NamedTemporaryFile() as f:
            os.chmod(f.name, 0606)
            cursor.execute("COPY ({}) TO '{}' WITH DELIMITER ',' CSV HEADER".format(query.query, f.name))

            response = HttpResponse(f.read(), content_type='text/csv')
            response['Content-Disposition'] = 'attachment;filename="variants.csv"'
            return response

    elif format == 'json':

        count = query.count()

        if search_term:
            # Number of synonym matches = total matches minus matches on "normal" columns
            synonyms = count - apply_search(query, search_term, search_column='fts_standard').count()
        else:
            synonyms = 0

        query = select_page(query, page_size, page_num)

        # call list() now to evaluate the query
        response = JsonResponse({'count': count, 'synonyms': synonyms, 'data': list(query.values(*column))})
        response['Access-Control-Allow-Origin'] = '*'
        return response


def apply_sources(query, include, exclude):
    # if there are multiple sources given then OR them:
    # the row must match in at least one column
    include_list = (Q(**{column: True}) for column in include)
    exclude_dict = {exclusion: False for exclusion in exclude}

    return query.filter(reduce(__or__, include_list)).filter(**exclude_dict)


def apply_filters(query, filterValues, filters, quotes=''):
    # if there are multiple filters the row must match all the filters
    for column, value in zip(filters, filterValues):
        if column == 'id':
            query = query.filter(**{column: value})
        else:
            query = query.extra(
                where=["\"{0}\" LIKE %s".format(column)],
                params=["{0}{1}%{0}".format(quotes, value)]
            )
    return query


def apply_search(query, search_term, search_column='fts_document', quotes=''):
    # search using the tsvector column which represents our document made of all the columns
    where_clause = "variant.{} @@ to_tsquery('simple', %s)".format(search_column)
    parameter = quotes + sanitise_term(search_term) + quotes
    return query.extra(
        where=[where_clause],
        params=[parameter]
    )


def apply_order(query, order_by, direction):
    # special case for HGVS columns
    if order_by in ('HGVS_cDNA', 'HGVS_Protein'):
        order_by = 'Genomic_Coordinate_hg38'
    if direction == 'descending':
        order_by = '-' + order_by
    return query.order_by(order_by, 'Pathogenicity_default')


def select_page(query, page_size, page_num):
    if page_size:
        start = page_size * page_num
        end = start + page_size
        return query[start:end]
    return query


def autocomplete(request):
    term = request.GET.get('term')
    limit = int(request.GET.get('limit', 10))

    cursor = connection.cursor()

    cursor.execute(
        """"SELECT word FROM words
        WHERE word LIKE %s
        AND char_length(word) >= 3
        ORDER BY word""",
        ["%s%%" % term])

    rows = cursor.fetchall()

    response = JsonResponse({'suggestions': rows[:limit]})
    response['Access-Control-Allow-Origin'] = '*'
    return response


def sanitise_term(term):
    # Escape all non alphanumeric characters
    term = re.escape(term)
    # Enable prefix search
    term += ":*"
    return term

########################### START WORK #####################################
########################### START WORK #####################################
########################### START WORK #####################################
@require_http_methods(["POST"])
def index_num_2(request):
    variant_set_id = request.POST.get('variantSetId')
    reference_name = request.POST.get('referenceName')
    start = request.POST.get('start')
    end = request.POST.get('end')
    page_size = request.POST.get('pageSize')
    page_token = request.POST.get('pageToken')

    response0 = v_s.SearchVariantsResponse()

    valid_resp, Bool = validate_responce(variant_set_id, reference_name, start , end)

    if Bool == False:
        return valid_resp

    elif Bool == True :

        response = vrs.Variant()
        response.id = "WyIxa2dlbm9tZXMiLCJ2cyIsInBoYXNlMy1yZWxlYXNlIiwiMTciLCIxMDAxMyIsIjE4NmY4YmU1NzE4NjlkN2NlMzJmODAzZTBkZTI2ZTk1Il0"
        response.variant_set_id = "WyIxa2dlbm9tZXMiLCJ2cyIsInBoYXNlMy1yZWxlYXNlIl0"
        response.names.append("rs139738597")
        response.created = 10
        response.updated = 0
        response.reference_name = "17"
        response.start = 10013
        response.end = 10014
        response.reference_bases = "C"
        response.alternate_bases.append("A")

        response0.variants.extend([response])
        resp = json_format._MessageToJsonObject(response0, False)
        return JsonResponse(resp)

    else:
        return JsonResponse({'variant_set_id':variant_set_id, 'reference_name':reference_name, 'start':start, 'end': end,'page_size' : page_size ,'page_token' : page_token})

def brca_to_ga4gh(brca_variant):

    request_response = v_s.SearchVariantsResponse()
    var_resp = vrs.Variant()

    for j in brca_variant:
        if j == "Genomic_Coordinate_hg37":
            var_resp.reference_name, start, bases = brca_variant[j].split(':')
            var_resp.reference_bases, alternbases = bases.split(">")
            for i in range(len(alternbases)):
                var_resp.alternate_bases.append(alternbases[i])
            var_resp.start = int(start)
            var_resp.end = var_resp.start+len(alternbases)
            var_resp.id = "This is ID"
            var_resp.variant_set_id = "brca_exchange"
            var_resp.names.append("This are names")
            var_resp.created = 0
            var_resp.updated = 0
        else:
            var_resp.info[str(j)].append(brca_variant[j])
    request_response.variants.extend([var_resp])

    return JsonResponse(json_format._MessageToJsonObject(request_response, False))


# def ga4gh_brca_page(query, page_size, page_num):
#     if page_size:
#         start = page_size * page_num    ######   UP NEXT
#         end = start + page_size         ######  SETTING UP
#         return query[start:end]         ######    PAGING
#     return query


def validate_responce(variant_set_id, reference_name, start, end):
    if variant_set_id == None:
        return JsonResponse({"error code": "400", "message": "invalid request: variant_set_id"  }), False
    elif reference_name == None:
        return JsonResponse({"error code": "400", "message": "invalid request: reference_name"}), False
    elif start == None :
        return JsonResponse({"error code": "400", "message": "invalid request: start"}), False
    elif end == None:
        return JsonResponse({"error code": "400", "message": "invalid request: end"}), False

    else:
        return "PASS", True

def data_response(request):
    varinat_set_id = request.POST.get('variantSetId')
    reference_name = request.POST.get('referenceName')
    start = request.POST.get('start')
    end = request.POST.get('end')
    page_token = request.POST.get('pageToken')

    return JsonResponse({'variant_set_id':varinat_set_id, 'reference_name':reference_name, 'start':start, 'end': end, 'page_token' : page_token})
