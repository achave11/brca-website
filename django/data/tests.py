import json
import os
import unittest
from urllib import quote


from django.http import JsonResponse, HttpResponse
from django.test import TestCase
from django.test.client import RequestFactory

from brca import settings
from data import test_data
from data.models import Variant
from data.views import index, autocomplete, data_response, index_num_2, brca_to_ga4gh

##################### MY EDITS #########################

from django.test import Client
c = Client()
import google.protobuf.json_format as json_format
from ga4gh import variant_service_pb2 as v_s
from ga4gh import variants_pb2 as vrs

##################### MY EDITS END#####################

class VariantTestCase(TestCase):
    def setUp(self):
        self.factory = RequestFactory()
        datafile = os.path.join(settings.BASE_DIR, 'data', 'resources', 'aggregated.tsv')
        self.db_size = sum(1 for _ in open(datafile)) - 1

    def test_variant_model(self):
        """Create a new variant and then retrieve it by the Genomic_Coordinate_hg38 column"""
        self.assertEqual(len(Variant.objects.all()), self.db_size)
        Variant.objects.create_variant(row=(test_data.new_variant()))
        self.assertEqual(len(Variant.objects.all()), self.db_size + 1)
        retrieved_variant = Variant.objects.get(Genomic_Coordinate_hg38="chr17:999999:A>G")
        self.assertIsNotNone(retrieved_variant)

    def test_index_resource_json(self):
        """Searching for all the data in json format returns a JsonResponse"""
        request = self.factory.get(
            '/data/?format=json&order_by=Gene_Symbol&direction=ascending&page_size=20&page_num=0&search_term=')
        response = index(request)

        self.assertIsInstance(response, JsonResponse)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(json.loads(response.content)['count'], self.db_size)

    def test_index_resource_csv(self):
        """Searching for all the data in csv format returns an HttpResponse"""
        request = self.factory.get(
            '/data/?format=json&order_by=Gene_Symbol&direction=ascending&page_size=20&page_num=0&search_term=')
        response = index(request)

        self.assertIsInstance(response, HttpResponse)
        self.assertEqual(response.status_code, 200)

    def search_by_id(self):
        """Searching for a variant by id using a filter should return the expected result"""
        existing_id = test_data.existing_variant()["id"]
        request = self.factory.get(
            '/data/?format=json&filter=id&filterValue=%s&order_by=Gene_Symbol&direction=ascending&page_size=20&page_num=0&search_term=' % existing_id)
        response = index(request)

        self.assertIsInstance(response, JsonResponse)
        self.assertEqual(response.status_code, 200)
        self.assertJSONEqual(response.content, {"count": 1, "data": [test_data.existing_variant()]})

    @unittest.skip("Not Passing")
    def test_autocomplete_nucleotide(self):
        """Getting autocomplete suggestions for words starting with c.2123 should return 2 results"""
        search_term = quote('c.2123')
        expected_autocomplete_results = [["c.2123c>a"], ["c.2123c>t"]]

        request = self.factory.get('/data/suggestions/?term=%s' % search_term)
        response = autocomplete(request)

        self.assertIsInstance(response, JsonResponse)
        self.assertEqual(response.status_code, 200)
        self.assertJSONEqual(response.content, {"suggestions": expected_autocomplete_results})
    @unittest.skip("Not Passing")
    def test_autocomplete_bic(self):
        """Getting autocomplete suggestions for words starting with IVS7+10 should return 2 results"""
        search_term = quote('ivs7+10')
        expected_autocomplete_results = [["ivs7+1028t>a"], ["ivs7+1037t>c"]]

        query = '/data/suggestions/?term=%s' % search_term
        request = self.factory.get(query)
        response = autocomplete(request)

        self.assertIsInstance(response, JsonResponse)
        self.assertEqual(response.status_code, 200)
        self.assertJSONEqual(response.content, {"suggestions": expected_autocomplete_results})

###################################################################################
############################# NEW TESTS START #####################################
    def test_ga4gh_variants_status_code(self):

        """" Getting a 200 status code returned !"""

        request0 =  self.factory.post("/data/ga4gh")#v0.6.5a/variants")
        response = data_response(request0)

        self.assertEqual(response.status_code, 200)

    def test_ga4gh_json_response(self):

        request = v_s.SearchVariantsRequest()
        request.variant_set_id = "NA21144"
        request.reference_name = "RefName###"
        request.start = 13
        request.end = 13131313
        request.page_size = 200
        request.page_token = '20'
        json_req = json_format._MessageToJsonObject(request, True)

        req = self.factory.post("/data/ga4gh", json_req)
        resp = data_response(req)

        result = {unicode('variant_set_id'): unicode("NA21144"), unicode('reference_name'): unicode("RefName###"), unicode('start'): unicode(13), unicode('end'): unicode(13131313), unicode('page_token'): unicode('20')}
        self.assertJSONEqual(resp.content, result)

    def test_validated_varSetId_request(self):
        request = v_s.SearchVariantsRequest()


        req = self.factory.post("/data/ga4gh", json_format._MessageToJsonObject(request, False))
        response = index_num_2(req)

        self.assertJSONEqual(response.content,{"error code": "400", "message": "invalid request: variant_set_id"  } )

    def test_validate_refName_request(self):
        request = v_s.SearchVariantsRequest()
        request.variant_set_id = "Something not null"


        req = self.factory.post("data/ga4gh", json_format._MessageToJsonObject(request, False) )
        response = index_num_2(req)

        self.assertJSONEqual(response.content, {"error code": "400", "message": "invalid request: reference_name"})

    def test_validate_start_request(self):
        request = v_s.SearchVariantsRequest()
        request.variant_set_id = "SOME-ID"
        request.reference_name = "SOME-REF-NAME"

        Jsonrequest = self.factory.post("data/ga4gh", json_format._MessageToJsonObject(request, False))
        responce = index_num_2(Jsonrequest)

        self.assertJSONEqual(responce.content, {"error code": "400", "message": "invalid request: start"})

    def test_validate_end_request(self):
        request = v_s.SearchVariantsRequest()
        request.variant_set_id = "SOME-ID"
        request.reference_name = "SOME-REF-NAME"
        request.start = 1

        Jsonrequest = self.factory.post("data/ga4gh", json_format._MessageToJsonObject(request, False))
        responce = index_num_2(Jsonrequest)

        self.assertJSONEqual(responce.content, {"error code": "400", "message": "invalid request: end"})

    def test_valid_response_returned(self):
        response0 = v_s.SearchVariantsResponse()

        var_resp = vrs.Variant()
        var_resp.id = "WyIxa2dlbm9tZXMiLCJ2cyIsInBoYXNlMy1yZWxlYXNlIiwiMTciLCIxMDAxMyIsIjE4NmY4YmU1NzE4NjlkN2NlMzJmODAzZTBkZTI2ZTk1Il0"
        var_resp.variant_set_id = "WyIxa2dlbm9tZXMiLCJ2cyIsInBoYXNlMy1yZWxlYXNlIl0"
        var_resp.names.append("rs139738597")
        var_resp.created = 10
        var_resp.updated = 0
        var_resp.reference_name = "17"
        var_resp.start = 10013
        var_resp.end = 10014
        var_resp.reference_bases = "C"
        var_resp.alternate_bases.append("A")
        response0.variants.extend([var_resp])
        expectedResp = json_format._MessageToJsonObject(response0, False)


        request = v_s.SearchVariantsRequest()
        request.variant_set_id = "SOME-ID"
        request.reference_name = "SOME-REF-NAME"
        request.start = 1
        request.end = 10


        Jsonrequest = self.factory.post("data/ga4gh", json_format._MessageToJsonObject(request, False))
        Jresponse = index_num_2(Jsonrequest)

        self.assertJSONEqual(Jresponse.content, expectedResp)

    def test_ga4gh_translator(self):
        response0 = v_s.SearchVariantsResponse()
        var_resp = vrs.Variant()

        Resp = Variant.objects.values()[0]

        for j in Resp:
            if j == "Genomic_Coordinate_hg37":
                var_resp.reference_name, start, bases = Resp[j].split(':')
                var_resp.reference_bases, alternbases = bases.split(">")
                for i in range(len(alternbases)):
                    var_resp.alternate_bases.append(alternbases[i])
                var_resp.start = int(start)
                var_resp.end = var_resp.start + len(alternbases)
                var_resp.id = "This is ID"
                var_resp.variant_set_id = "brca_exchange"
                var_resp.names.append("This are names")
                var_resp.created = 0
                var_resp.updated = 0
            else:
                var_resp.info[str(j)].append(Resp[j])
        response0.variants.extend([var_resp])

        expectedResp = json_format._MessageToJsonObject(response0, False)

        request = Variant.objects.values()[0]
        Jresponse = brca_to_ga4gh(request)

        self.assertJSONEqual(Jresponse.content, expectedResp)

    #def tets_GA4GH_translator(self):

if __name__ == '__main__':
    unittest.main()
