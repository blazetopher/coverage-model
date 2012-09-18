#!/usr/bin/env python

"""
@package coverage_model.parameter_values
@file coverage_model/parameter_values.py
@author Christopher Mueller
@brief Abstract and concrete value objects for parameters
"""
from pyon.public import log
from coverage_model.basic_types import AbstractBase, is_valid_constraint
import numpy as np
import numexpr as ne

def get_value_class(param_type, **kwargs):
    module = __import__(param_type._value_module, fromlist=[param_type._value_class])
    classobj = getattr(module, param_type._value_class)
    return classobj(parameter_type=param_type, **kwargs)

#=========================
# Abstract Parameter Value Objects
#=========================

class AbstractParameterValue(AbstractBase):
    """

    """
    def __init__(self, parameter_type, domain_set, **kwargs):
        """

        @param **kwargs Additional keyword arguments are copied and the copy is passed up to AbstractBase; see documentation for that class for details
        """
        kwc=kwargs.copy()
        AbstractBase.__init__(self, **kwc)
        self.parameter_type = parameter_type
        self.domain_set = domain_set
        self._value=[]

    def _fix_slice(self, slice_, rank):
        # CBM: First swack - see this for more possible checks: http://code.google.com/p/netcdf4-python/source/browse/trunk/netCDF4_utils.py
        if not is_valid_constraint(slice_):
            raise SystemError('invalid constraint supplied: {0}'.format(slice_))

        # First, ensure we're working with a tuple
        if not np.iterable(slice_):
            slice_ = (slice_,)
        elif not isinstance(slice_,tuple):
            slice_ = tuple(slice_)

        # Then make it's the correct rank
        slen = len(slice_)
        if not slen == rank:
            if slen > rank:
                slice_ = slice_[:rank]
            else:
                for n in range(slen, rank):
                    slice_ += (slice(None,None,None),)

        return slice_

    @property
    def shape(self):
        return self.domain_set.total_extents

    @property
    def content(self):
        return self._value

    def expand_content(self, domain, origin, expansion):
        raise NotImplementedError('Not implemented by abstract class')

    def __len__(self):
        return len(self._value)

class AbstractSimplexParameterValue(AbstractParameterValue):
    """

    """
    def __init__(self, parameter_type, domain_set, **kwargs):
        """

        @param **kwargs Additional keyword arguments are copied and the copy is passed up to AbstractParameterValue; see documentation for that class for details
        """
        kwc=kwargs.copy()
        AbstractParameterValue.__init__(self, parameter_type, domain_set, **kwc)


class AbstractComplexParameterValue(AbstractParameterValue):
    """

    """
    def __init__(self, parameter_type, domain_set, **kwargs):
        """

        @param **kwargs Additional keyword arguments are copied and the copy is passed up to AbstractParameterValue; see documentation for that class for details
        """
        kwc=kwargs.copy()
        AbstractParameterValue.__init__(self, parameter_type, domain_set, **kwc)

#=========================
# Concrete Parameter Value Objects
#=========================

class NumericValue(AbstractSimplexParameterValue):

    def __init__(self, parameter_type, domain_set, **kwargs):
        """

        @param **kwargs Additional keyword arguments are copied and the copy is passed up to AbstractSimplexParameterValue; see documentation for that class for details
        """
        kwc=kwargs.copy()
        AbstractSimplexParameterValue.__init__(self, parameter_type, domain_set, **kwc)
        self._value = np.empty(self.shape, dtype=self.parameter_type.value_encoding)
        self._value.fill(self.parameter_type.fill_value)

#    @property
#    def content(self):
#        return self._value

#    @content.setter
#    def content(self, value):
#        self._value=value

    def expand_content(self, domain, origin, expansion):
        if domain == self.domain_set.tdom: # Temporal
            narr = np.empty(self._value.shape[1:], dtype=self._value.dtype)
            narr.fill(self.parameter_type.fill_value)
            loc=[origin for x in xrange(expansion)]
            self._value = np.insert(self._value, loc, narr, axis=0)
        elif domain == self.domain_set.sdom: # Spatial
            raise NotImplementedError('Expansion of the Spatial Domain is not yet supported')

    def __getitem__(self, slice_):
        slice_ = self._fix_slice(slice_, len(self.shape))

        return self._value[slice_]

    def __setitem__(self, slice_, value):
        slice_ = self._fix_slice(slice_, len(self.shape))

        self._value[slice_] = value

class FunctionValue(AbstractComplexParameterValue):
    # CBM TODO: There are 2 'classes' of Function - those that operate against an INDEX, and those that operate against a VALUE
    # CBM TODO: Does this actually end up as a subclass of VectorValue?  basically a 2 member tuple?

    def __init__(self, parameter_type, domain_set, **kwargs):
        """

        @param **kwargs Additional keyword arguments are copied and the copy is passed up to AbstractComplexParameterValue; see documentation for that class for details
        """
        kwc=kwargs.copy()
        AbstractComplexParameterValue.__init__(self, parameter_type, domain_set, **kwc)

        self._value = np.empty([0], dtype=self.parameter_type.value_encoding)

    @property
    def content(self):
        return self._value[-1]
#        if len(self._value) > 1:
#            # CBM TODO: Add logic to deal with this - need to check the value on teh way in to see if it's a 'where'?
#            raise NotImplementedError('Not there yet')
#        else:
#            return self._value[0]

    def expand_content(self, domain, origin, expansion):
        # No op - appropriate domain applied during retrieval of data
        pass

    def __getitem__(self, slice_):
        slice_ = self._fix_slice(slice_, len(self.shape))

        fill_val = self.parameter_type.fill_value

        value = self._value[-1]
        if value.startswith('c'):
            c = np.ones(self.shape)[slice_]
        else:
            #            x = np.arange(sum(slice_)).reshape(slice_)
            raise NotImplementedError('Not there yet')

        return ne.evaluate(self.content).astype(self.parameter_type.value_encoding)

    def __setitem__(self, slice_, value):
        self._value = np.append(self._value, value)

class ConstantValue(AbstractComplexParameterValue):

    def __init__(self, parameter_type, domain_set, **kwargs):
        """

        @param **kwargs Additional keyword arguments are copied and the copy is passed up to AbstractComplexParameterValue; see documentation for that class for details
        """
        kwc=kwargs.copy()
        AbstractComplexParameterValue.__init__(self, parameter_type, domain_set, **kwc)

        self._value = None

    @property
    def content(self):
        return self._value

    def expand_content(self, domain, origin, expansion):
        # No op - constant over any domain
        pass

    def __getitem__(self, slice_):
        slice_ = self._fix_slice(slice_, len(self.shape))

        c = np.ones(self.shape)[slice_] # Make an array of ones of the appropriate shape and slice it as desired

        return ne.evaluate(self._value).astype(self.parameter_type.value_encoding)

    def __setitem__(self, slice_, value):
        value = str(value) # Ensure we're dealing with a str
        if self.parameter_type.is_valid_value(value):
            if not value.startswith('c*'):
                value = 'c*'+str(value)
            self._value = value

class RecordValue(AbstractComplexParameterValue):

    def __init__(self, parameter_type, domain_set, **kwargs):
        """

        @param **kwargs Additional keyword arguments are copied and the copy is passed up to AbstractComplexParameterValue; see documentation for that class for details
        """
        kwc=kwargs.copy()
        AbstractComplexParameterValue.__init__(self, parameter_type, domain_set, **kwc)

        self._value = np.empty(self.shape, dtype=object)

    def expand_content(self, domain, origin, expansion):
        if domain == self.domain_set.tdom: # Temporal
            narr = np.empty(self._value.shape[1:], dtype=self._value.dtype)
            narr.fill(self.parameter_type.fill_value)
            loc=[origin for x in xrange(expansion)]
            self._value = np.insert(self._value, loc, narr, axis=0)
        elif domain == self.domain_set.sdom: # Spatial
            raise NotImplementedError('Expansion of the Spatial Domain is not yet supported')

    def __getitem__(self, slice_):
        slice_ = self._fix_slice(slice_, len(self.shape))

        ret = self._value[slice_]

        return ret

    def __setitem__(self, slice_, value):
        slice_ = self._fix_slice(slice_, len(self.shape))

        self._value[slice_] = value

class VectorValue(AbstractComplexParameterValue):

    def __init__(self, parameter_type, domain_set, **kwargs):
        """

        @param **kwargs Additional keyword arguments are copied and the copy is passed up to AbstractComplexParameterValue; see documentation for that class for details
        """
        kwc=kwargs.copy()
        AbstractComplexParameterValue.__init__(self, parameter_type, domain_set, **kwc)

class ArrayValue(AbstractComplexParameterValue):

    def __init__(self, parameter_type, domain_set, **kwargs):
        """

        @param **kwargs Additional keyword arguments are copied and the copy is passed up to AbstractComplexParameterValue; see documentation for that class for details
        """
        kwc=kwargs.copy()
        AbstractComplexParameterValue.__init__(self, parameter_type, domain_set, **kwc)

        self._value = np.empty(self.shape, dtype=object)

    def expand_content(self, domain, origin, expansion):
        if domain == self.domain_set.tdom: # Temporal
            narr = np.empty(self._value.shape[1:], dtype=self._value.dtype)
            narr.fill(self.parameter_type.fill_value)
            loc=[origin for x in xrange(expansion)]
            self._value = np.insert(self._value, loc, narr, axis=0)
        elif domain == self.domain_set.sdom: # Spatial
            raise NotImplementedError('Expansion of the Spatial Domain is not yet supported')

    def __getitem__(self, slice_):
        slice_ = self._fix_slice(slice_, len(self.shape))

        ret = self._value[slice_]

        return ret

    def __setitem__(self, slice_, value):
        slice_ = self._fix_slice(slice_, len(self.shape))

        self._value[slice_] = value