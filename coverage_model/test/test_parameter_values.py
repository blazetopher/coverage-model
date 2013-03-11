#!/usr/bin/env python

"""
@package
@file test_parameter_values.py
@author James D. Case
@brief
"""

from nose.plugins.attrib import attr
from coverage_model import *
import numpy as np
import random

# TODO: Revisit this test class and expand/elaborate the tests


@attr('UNIT',group='cov')
class TestParameterValuesUnit(CoverageModelUnitTestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    # QuantityType
    def test_quantity_values(self):
        num_rec = 10
        dom = SimpleDomainSet((num_rec,))

        qtype = QuantityType(value_encoding=np.dtype('float32'))
        qval = get_value_class(qtype, domain_set=dom)

        data = np.arange(10)
        qval[:] = data
        self.assertTrue(np.array_equal(data, qval[:]))

    # BooleanType
    def test_boolean_values(self):
        num_rec = 10

        dom = SimpleDomainSet((num_rec,))

        btype = BooleanType()
        bval = get_value_class(btype, domain_set=dom)

        data = [True, False, True, False, False, True, False, True, True, True]
        bval[:] = data
        self.assertTrue(np.array_equal(data, bval[:]))

    # ArrayType
    def test_array_values(self):
        num_rec = 10
        dom = SimpleDomainSet((num_rec,))

        atype = ArrayType()
        aval = get_value_class(atype, domain_set=dom)

        for x in xrange(num_rec):
            aval[x] = np.random.bytes(np.random.randint(1,20)) # One value (which is a byte string) for each member of the domain

        self.assertIsInstance(aval[0], np.ndarray)
        self.assertIsInstance(aval[0][0], basestring)
        self.assertTrue(1 <= len(aval[0][0]) <= 20)

        vals = [[1, 2, 3]] * num_rec
        val_arr = np.empty(num_rec, dtype=object)
        val_arr[:] = vals

        aval[:] = vals
        self.assertIsInstance(aval[0], list)
        self.assertTrue(np.array_equal(aval[:], val_arr))
        self.assertEqual(aval[0], [1, 2, 3])

        aval[:] = val_arr
        self.assertIsInstance(aval[0], list)
        self.assertTrue(np.array_equal(aval[:], val_arr))
        self.assertEqual(aval[0], [1, 2, 3])


    # RecordType
    def test_record_values(self):
        num_rec = 10
        dom = SimpleDomainSet((num_rec,))

        rtype = RecordType()
        rval = get_value_class(rtype, domain_set=dom)

        letts='abcdefghij'
        for x in xrange(num_rec):
            rval[x] = {letts[x]: letts[x:]} # One value (which is a dict) for each member of the domain

        self.assertIsInstance(rval[0], dict)

    # ConstantType
    def test_constant_values(self):
        num_rec = 10
        dom = SimpleDomainSet((num_rec,))

        ctype = ConstantType(QuantityType(value_encoding=np.dtype('int32')))
        cval = get_value_class(ctype, domain_set=dom)
        cval[0] = 200 # Doesn't matter what index (or indices) you assign this to - it's used everywhere!!
        self.assertEqual(cval[0], 200)
        self.assertEqual(cval[7], 200)
        self.assertEqual(cval[2,9], 200)
        self.assertTrue(np.array_equal(cval[[2,7],], np.array([200,200], dtype='int32')))

    # ConstantRangeType
    def test_constant_range_values(self):
        num_rec = 10
        dom = SimpleDomainSet((num_rec,))

        crtype = ConstantRangeType(QuantityType(value_encoding=np.dtype('int16')))
        crval = get_value_class(crtype, domain_set=dom)
        crval[:] = (-10, 10)
        self.assertEqual(crval[0], (-10, 10))
        self.assertEqual(crval[6], (-10, 10))
        comp=np.empty(2,dtype='object')
        comp.fill((-10,10))

        self.assertTrue(np.array_equal(crval[[2,7],], comp))

    # CategoryType
    def test_category_values(self):
        num_rec = 10
        dom = SimpleDomainSet((num_rec,))

        # CategoryType example
        cat = {0:'turkey',1:'duck',2:'chicken',99:'None'}
        cattype = CategoryType(categories=cat)
        catval = get_value_class(cattype, domain_set=dom)

        catkeys = cat.keys()
        for x in xrange(num_rec):
            catval[x] = random.choice(catkeys)

        with self.assertRaises(IndexError):
            catval[20]

        self.assertTrue(catval[0] in cat.values())

    # FunctionType
    def test_function_values(self):
        num_rec = 10
        dom = SimpleDomainSet((num_rec,))

        ftype = FunctionType(QuantityType(value_encoding=np.dtype('float32')))
        fval = get_value_class(ftype, domain_set=dom)

        fval[:] = make_range_expr(100, min=0, max=4, min_incl=True, max_incl=False, else_val=-9999)
        fval[:] = make_range_expr(200, min=4, max=6, min_incl=True, else_val=-9999)
        fval[:] = make_range_expr(300, min=6, else_val=-9999)

        self.assertEqual(fval[0], 100)
        self.assertEqual(fval[5], 200)
        self.assertEqual(fval[9], 300)

    def test_parameter_function_values(self):
        pass

    def test_sparse_constant_value(self):
        num_rec = 0
        dom = SimpleDomainSet((num_rec,))
        sctype = SparseConstantType(fill_value=-999)
        scval = get_value_class(sctype, dom)

        scval[:] = 10
        self.assertTrue(np.array_equal(scval[:], np.empty(0, dtype=sctype.value_encoding)))

        dom.shape = (10,)
        self.assertTrue(np.array_equal(scval[:], np.array([10] * 10, dtype=sctype.value_encoding)))

        scval[:] = 20
        dom.shape = (20,)
        out = np.empty(20, dtype=sctype.value_encoding)
        out[:10] = 10
        out[10:] = 20
        self.assertTrue(np.array_equal(scval[:], out))
        self.assertTrue(np.array_equal(scval[2:19], out[2:19]))
        self.assertTrue(np.array_equal(scval[8::3], out[8::3]))

        scval[:] = 30
        dom.shape = (30,)
        out = np.empty(30, dtype=sctype.value_encoding)
        out[:10] = 10
        out[10:20] = 20
        out[20:] = 30
        self.assertTrue(np.array_equal(scval[:], out))
        self.assertTrue(np.array_equal(scval[2:29], out[2:29]))
        self.assertTrue(np.array_equal(scval[12:25], out[12:25]))
        self.assertTrue(np.array_equal(scval[18::3], out[18::3]))


@attr('INT',group='cov')
class TestParameterValuesInt(CoverageModelIntTestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_value_caching(self):
        import collections

        cov = self._make_empty_oneparamcov()

        # Insert some timesteps (automatically expands other arrays)
        nt = 2000
        cov.insert_timesteps(nt)

        vals = np.arange(nt, dtype=cov._range_dictionary.get_context('time').param_type.value_encoding)
        cov.set_time_values(vals)

        # Make sure the _value_cache is an instance of OrderedDict and that it's empty
        self.assertIsInstance(cov._value_cache, collections.OrderedDict)
        self.assertEqual(len(cov._value_cache), 0)

        # Get the time values and make sure they match what we assigned
        got = cov.get_time_values()
        self.assertTrue(np.array_equal(vals, got))

        # Now check that there is 1 entry in the _value_cache and that it's a match for vals
        self.assertEqual(len(cov._value_cache), 1)
        self.assertTrue(np.array_equal(cov._value_cache[cov._value_cache.keys()[0]], vals))

        # Now retrieve a slice and make sure it matches the same slice of vals
        sl = slice(20, 1000, 3)
        got = cov.get_time_values(sl)
        self.assertTrue(np.array_equal(vals[sl], got))

        # Now check that there are 2 entries and that the second is a match for vals
        self.assertEqual(len(cov._value_cache), 2)
        self.assertTrue(np.array_equal(cov._value_cache[cov._value_cache.keys()[1]], vals[sl]))

        # Call get 40 times - check that the _value_cache stops growing @ 30
        for x in xrange(40):
            cov.get_time_values(x)
        self.assertEqual(len(cov._value_cache), 30)

    def test_value_caching_with_domain_expansion(self):
        cov = self._make_empty_oneparamcov()

        # Insert some timesteps (automatically expands other arrays)
        nt = 100
        cov.insert_timesteps(nt)

        vals = np.arange(nt, dtype=cov._range_dictionary.get_context('time').param_type.value_encoding)
        cov.set_time_values(vals)

        # Prime the value_cache
        got = cov.get_time_values()

        # Expand the domain
        cov.insert_timesteps(nt)

        # Value cache should still hold 1 and the value should be equal to values retrieved prior to expansion ('got')
        self.assertEqual(len(cov._value_cache), 1)
        self.assertTrue(np.array_equal(cov._value_cache[cov._value_cache.keys()[0]], got))

        # Perform another get, just to make sure the following removes all entries for the parameter
        got = cov.get_time_values(slice(0, 10))

        # Set time values
        cov.set_time_values(range(cov.num_timesteps))

        # Value cache should now be empty because all values cached for 'time' should be removed
        self.assertEqual(len(cov._value_cache), 0)

    def _make_empty_oneparamcov(self):
        # Instantiate a ParameterDictionary
        pdict = ParameterDictionary()

        # Create a set of ParameterContext objects to define the parameters in the coverage, add each to the ParameterDictionary
        t_ctxt = ParameterContext('time', param_type=QuantityType(value_encoding=np.dtype('int64')))
        t_ctxt.axis = AxisTypeEnum.TIME
        t_ctxt.uom = 'seconds since 01-01-1970'
        pdict.add_context(t_ctxt)

        # Construct temporal and spatial Coordinate Reference System objects
        tcrs = CRS([AxisTypeEnum.TIME])
        scrs = CRS([AxisTypeEnum.LON, AxisTypeEnum.LAT])

        # Construct temporal and spatial Domain objects
        tdom = GridDomain(GridShape('temporal', [0]), tcrs, MutabilityEnum.EXTENSIBLE) # 1d (timeline)
        sdom = GridDomain(GridShape('spatial', [0]), scrs, MutabilityEnum.IMMUTABLE) # 0d spatial topology (station/trajectory)

        # Instantiate the SimplexCoverage providing the ParameterDictionary, spatial Domain and temporal Domain
        scov = SimplexCoverage('test_data', create_guid(), 'sample coverage_model', parameter_dictionary=pdict, temporal_domain=tdom, spatial_domain=sdom)

        return scov

    def _interop_assertions(self, cov, pname, val_cls):
        self.assertTrue(np.array_equal(cov.get_parameter_values(pname), val_cls[:]))
        self.assertTrue(np.array_equal(cov.get_parameter_values(pname, slice(-1, None)), val_cls[-1:]))
        self.assertTrue(np.array_equal(cov.get_parameter_values(pname, slice(None, None, 3)), val_cls[::3]))
        self.assertEqual(cov.get_parameter_values(pname, 0), val_cls[0])
        self.assertEqual(cov.get_parameter_values(pname, -1), val_cls[-1])

    def test_array_value_interop(self):
        # Setup the type
        arr_type = ArrayType()

        # Setup the values
        ntimes = 20
        vals = [[1, 2, 3]] * ntimes
        vals_arr = np.empty(ntimes, dtype=object)
        vals_arr[:] = vals

        # Setup the in-memory value
        dom = SimpleDomainSet((ntimes,))
        arr_val = get_value_class(arr_type, dom)

        # Setup the coverage
        pdict = ParameterDictionary()
        # Create a set of ParameterContext objects to define the parameters in the coverage, add each to the ParameterDictionary
        pdict.add_context(ParameterContext('time', param_type=QuantityType(value_encoding=np.dtype('int64'))), is_temporal=True)
        pdict.add_context(ParameterContext('array', param_type=arr_type))
        tdom = GridDomain(GridShape('temporal', [0]), CRS([AxisTypeEnum.TIME]), MutabilityEnum.EXTENSIBLE)

        # Instantiate the SimplexCoverage providing the ParameterDictionary, spatial Domain and temporal Domain
        cov = SimplexCoverage(self.working_dir, create_guid(), 'sample coverage_model', parameter_dictionary=pdict, temporal_domain=tdom)
        cov.insert_timesteps(ntimes)

        # Nested List Assignment
        arr_val[:] = vals
        cov.set_parameter_values('array', vals)
        self._interop_assertions(cov, 'array', arr_val)

        # Array Assignment
        arr_val[:] = vals_arr
        cov.set_parameter_values('array', vals_arr)
        self._interop_assertions(cov, 'array', arr_val)

    def test_category_value_interop(self):
        # Setup the type
        cats = {0: 'turkey', 1: 'duck', 2: 'chicken', 3: 'empty'}
        cat_type = CategoryType(categories=cats)
        cat_type.fill_value = 3

        # Setup the values
        ntimes = 10
        key_vals = [1, 2, 0, 3, 2, 0, 1, 2, 1, 1]
        cat_vals = [cats[k] for k in key_vals]
        key_vals_arr = np.array(key_vals)
        cat_vals_arr = np.empty(ntimes, dtype=object)
        cat_vals_arr[:] = cat_vals

        # Setup the in-memory value
        dom = SimpleDomainSet((ntimes,))
        cat_val = get_value_class(cat_type, dom)

        # Setup the coverage
        pdict = ParameterDictionary()
        # Create a set of ParameterContext objects to define the parameters in the coverage, add each to the ParameterDictionary
        pdict.add_context(ParameterContext('time', param_type=QuantityType(value_encoding=np.dtype('int64'))), is_temporal=True)
        pdict.add_context(ParameterContext('category', param_type=cat_type))
        tdom = GridDomain(GridShape('temporal', [0]), CRS([AxisTypeEnum.TIME]), MutabilityEnum.EXTENSIBLE)

        # Instantiate the SimplexCoverage providing the ParameterDictionary, spatial Domain and temporal Domain
        cov = SimplexCoverage(self.working_dir, create_guid(), 'sample coverage_model', parameter_dictionary=pdict, temporal_domain=tdom)
        cov.insert_timesteps(ntimes)

        # Assign with a list of keys
        cat_val[:] = key_vals
        cov.set_parameter_values('category', key_vals)
        self._interop_assertions(cov, 'category', cat_val)

        # Assign with a list of categories
        cat_val[:] = cat_vals
        cov.set_parameter_values('category', cat_vals)
        self._interop_assertions(cov, 'category', cat_val)

        # Assign with an array of keys
        cat_val[:] = key_vals_arr
        cov.set_parameter_values('category', key_vals_arr)
        self._interop_assertions(cov, 'category', cat_val)

        # Assign with an array of categories
        cat_val[:] = cat_vals_arr
        cov.set_parameter_values('category', cat_vals_arr)
        self._interop_assertions(cov, 'category', cat_val)


