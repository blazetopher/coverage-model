#!/usr/bin/env python

"""
@package ion_functions.test.base_test
@file ion_functions/test/base_test.py
@author Christopher Mueller
@brief Base class for Unit tests in ion_functions
"""

from nose.plugins.attrib import attr
from ion_functions.test.base_test import BaseUnitTestCase

import numpy as np
from ion_functions import qc_functions as qcf

@attr('UNIT', group='qc')
class TestQCFunctionsUnit(BaseUnitTestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_dataqc_globalrangetest(self):
        """
        Test as defined in DPS:
        https://alfresco.oceanobservatories.org/alfresco/d/d/workspace/SpacesStore/466c4915-c777-429a-8946-c90a8f0945b0/1341-10004_Data_Product_SPEC_GLBLRNG_OOI.pdf

        Table 1:Test Data Set
        x     lim    qcflag
        9   [10 20]    0
        10  [10 20]    1
        16  [10 20]    1
        17  [10 20]    1
        18  [10 20]    1
        19  [10 20]    1
        20  [10 20]    1
        25  [10 20]    0

        """
        x = [9, 10, 16, 17, 18, 19, 20, 25]
        lim = [10, 20]
        out = [0, 1, 1, 1, 1, 1, 1, 0]

        got = qcf.dataqc_globalrangetest(x, lim)

        self.assertTrue(np.array_equal(got, out))

    def test_dataqc_spiketest(self):
        """
        Test as defined in DPS:
        https://alfresco.oceanobservatories.org/alfresco/d/d/workspace/SpacesStore/eadad62c-ec80-403d-b3d3-c32c79f9e9e4/1341-10006_Data_Product_SPEC_SPKETST_OOI.pdf

        Table 1: Test Data Set
        dat  acc  N  L  out
        -4   0.1  5  5   1
        3                1
        40               0
        -1               1
        1                1
        -6               1
        -6               1
        1                1

        """

        dat = [-1, 3, 40, -1, 1, -6, -6, 1]
        acc = 0.1
        N = 5
        L = 5
        out = [1, 1, 0, 1, 1, 1, 1, 1]

        got = qcf.dataqc_spiketest(dat, acc, N, L)

        self.assertTrue(np.array_equal(got, out))

    def test_dataqc_stuckvaluetest(self):
        """
        Test as defined in DPS:
        https://alfresco.oceanobservatories.org/alfresco/d/d/workspace/SpacesStore/a04acb56-7e27-48c6-a40b-9bb9374ee35c/1341-10008_Data_Product_SPEC_STUCKVL_OOI.pdf

        x = [4.83  1.40  3.33  3.33  3.33  3.33  4.09  2.97  2.85  3.67]
        reso = 0.001
        num = 4

        out = 1     1     0     0     0     0     1     1     1     1

        @return:
        """
        x = [4.83, 1.40, 3.33, 3.33, 3.33, 3.33, 4.09, 2.97, 2.85, 3.67]
        reso = 0.001
        num = 4
        out = [1, 1, 0, 0, 0, 0, 1, 1, 1, 1]

        got = qcf.dataqc_stuckvaluetest(x, reso, num)

        self.assertTrue(np.array_equal(got, out))

    def test_dataqc_polytrendtest(self):
        """
        Test as defined in DPS:
        https://alfresco.oceanobservatories.org/alfresco/d/d/workspace/SpacesStore/c33037ab-9dd5-4615-8218-0957f60a47f3/1341-10007_Data_Product_SPEC_TRNDTST_OOI.pdf

        4.6 Code Verification and Test Data Sets
        The algorithm code will be verified using the test data set provided, which contains inputs and
        their associated correct outputs. CI will verify that the algorithm code is correct by checking that
        the algorithm pressure output, generated using the test data inputs, is identical to the test data
        output.
        n = 1
        nstd = 3

        x_in = 0.8147, 0.9058, 0.1270, 0.9134, 0.6324, 0.0975, 0.2785, 0.5469, 0.9575, 0.9649
        t = 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        x_out = 1


        x_in = 0.6557, 0.2357, 1.2491, 1.5340, 1.4787, 1.7577, 1.9431, 1.7922, 2.2555, 1.9712
        t = 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        x_out = 1


        x_in = 0.7060, 0.5318, 1.2769, 1.5462, 2.0971, 3.3235, 3.6948, 3.8171, 4.9502, 4.5344
        t = 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        x_out = 0


        x_in = 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        t = 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        x_out = 1


        x_in = 1, 1, 1, 1, 1, 1, 1, 1, 1, 1
        t = 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        x_out = 1


        x_in = 0.4387, -0.1184, -0.2345, -0.7048, -1.8131, -2.0102, -2.5544, -2.8537, -3.2906, -3.7453
        t = 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        x_out = 0


        @return:
        """
        n = 1
        nstd = 3

        x_in = [0.8147, 0.9058, 0.1270, 0.9134, 0.6324, 0.0975, 0.2785, 0.5469, 0.9575, 0.9649]
        t = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        x_out = 1

        got = qcf.dataqc_polytrendtest(x_in, t, n, nstd)
        self.assertTrue(np.array_equal(got, x_out))

        x_in = [0.6557, 0.2357, 1.2491, 1.5340, 1.4787, 1.7577, 1.9431, 1.7922, 2.2555, 1.9712]
        t = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        x_out = 1

        got = qcf.dataqc_polytrendtest(x_in, t, n, nstd)
        self.assertTrue(np.array_equal(got, x_out))

        x_in = [0.7060, 0.5318, 1.2769, 1.5462, 2.0971, 3.3235, 3.6948, 3.8171, 4.9502, 4.5344]
        t = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        x_out = 0

        got = qcf.dataqc_polytrendtest(x_in, t, n, nstd)
        self.assertTrue(np.array_equal(got, x_out))

        x_in = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        t = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        x_out = 1

        got = qcf.dataqc_polytrendtest(x_in, t, n, nstd)
        self.assertTrue(np.array_equal(got, x_out))

        x_in = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        t = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        x_out = 1

        got = qcf.dataqc_polytrendtest(x_in, t, n, nstd)
        self.assertTrue(np.array_equal(got, x_out))

        x_in = [0.4387, -0.1184, -0.2345, -0.7048, -1.8131, -2.0102, -2.5544, -2.8537, -3.2906, -3.7453]
        t = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        x_out = 0

        got = qcf.dataqc_polytrendtest(x_in, t, n, nstd)
        self.assertTrue(np.array_equal(got, x_out))



