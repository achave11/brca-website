import os
import re
import tempfile
import json
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
        """SELECT word FROM words
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

########################### START WORK ####################################
###########################################################################
###########################################################################
end_index = None
@require_http_methods(["POST"])
def index_num_2(request):
    conditional = validate_request(request)
    if conditional :
        return conditional

    else:
        req_dict = json.loads(request.body)
        variant_set_id = req_dict.get('variantSetId')
        reference_name = req_dict.get('referenceName')
        start = req_dict.get('start')
        end = req_dict.get('end')
        page_size = req_dict.get('pageSize', 1)
        page_token = req_dict.get('pageToken', "0")

    response0 = v_s.SearchVariantsResponse()

    filt = str(reference_name)+":"
    DbResp = Variant.objects
    DbResp = DbResp.filter(Genomic_Coordinate_hg37__startswith=filt)
    ret_data = ga4gh_brca_page(DbResp, int(page_size), int(page_token))
    ga_vars = [brca_to_ga4gh(i) for i in ret_data.values()]
    if len(ga_vars) > page_size:
        ga_vars.pop()
        page_token = str(1 + int(page_token))
    else:
        response0.next_page_token = ""
    response0.variants.extend(ga_vars)
    response0.next_page_token = page_token
    resp = json_format._MessageToJsonObject(response0, True)
    return JsonResponse(resp)

def ga4gh_brca_page(query, page_size, page_token):
    start = page_size * page_token
    end = start + page_size+1
    return query[start:end]

def brca_to_ga4gh(brca_variant):
    var_resp = vrs.Variant()
    for j in brca_variant:
        if j == "Genomic_Coordinate_hg37":
            var_resp.reference_name, start, bases = brca_variant[j].split(':')
            var_resp.reference_bases, alternbases = bases.split(">")
            for i in range(len(alternbases)):
                var_resp.alternate_bases.append(alternbases[i])
            var_resp.start = int(start)
            var_resp.end = var_resp.start+len(alternbases)
            continue
        if j == "id":
            var_resp.id = str(brca_variant['id'])
            var_resp.variant_set_id = "brca_exchange_hg37"
            var_resp.names.append("This are names")
            var_resp.created = 0
            var_resp.updated = 0
            continue
        else:
            var_resp.info[str(j)].append(brca_variant[j])

    return var_resp

def validate_request(request):
    request
    if not request.body:
        return JsonResponse(ErrorMessages['emptyBody'])
            #{"error code": "400", "message": "invalid request: empty request"  }),
    else:
        request_dict = json.loads(request.body)
        if not request_dict.get("variantSetId"):
            return JsonResponse(ErrorMessages['VariantSetId'])
            #return JsonResponse({'error code': "400", 'message': "invalid request: variant_set_id" })
        elif not request_dict.get('referenceName'):
            return JsonResponse(ErrorMessages['referenceName'])
            #{"error code": "400", "message": "invalid request: reference_name"})
        elif not request_dict.get('start')  :
            return JsonResponse(ErrorMessages['start'])
                #{"error code": "400", "message": "invalid request: start"})
        elif not request_dict.get('end') :
            return JsonResponse(ErrorMessages['end'])
                #{"error code": "400", "message": "invalid request: end"})
        else:
            return None

ErrorMessages = {'emptyBody' :{'error code': 400, 'message' : 'invalid request empty request'},
                'VariantSetId' : {'error code': 400, 'message': 'invalid request variant_set_id'},
                 'referenceName': {'error code': 400, 'message': 'invalid request reference_name'},
                 'start': {'error code' : 400, 'message': 'invalid request start'},
                 'end' : {'error code' :400, 'message': 'invalid request end'}}