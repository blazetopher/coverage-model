#!/usr/bin/env python

"""
@package ion_functions.qc_functions
@file ion_functions/qc_functions.py
@author Christopher Mueller
@brief Module containing QC functions ported from matlab samples in DPS documents
"""

import numpy as np

ALL_KINDS = ('i', 'u', 'f', 'c', 'S', 'a', 'U')  # Does not include 'V' which is raw (void) or O which is object
NUMERIC_KINDS = ('i', 'u', 'f', 'c')
REAL_KINDS = ('i', 'u', 'f', 'S', 'a', 'U')  # All kinds but complex


def isnumeric(dat):
    """
    isnumeric - Determine whether input is numeric
    Syntax
    tf = isnumeric(A)
    Description
    tf = isnumeric(A) returns logical 1 (true) if A is a numeric array and logical 0 (false)
    otherwise. For example, sparse arrays and double-precision arrays are numeric, while strings,
    cell arrays, and structure arrays and logicals are not.
    Examples
    Given the following cell array,
    C{1,1} = pi;                 % double
    C{1,2} = 'John Doe';         % char array
    C{1,3} = 2 + 4i;             % complex double
    C{1,4} = ispc;               % logical
    C{1,5} = magic(3)            % double array
    C =
       [3.1416] 'John Doe' [2.0000+ 4.0000i] [1][3x3 double]
    isnumeric shows that all but C{1,2} and C{1,4} are numeric arrays.
    for k = 1:5
    x(k) = isnumeric(C{1,k});
    end
    x
    x =
         1     0     1     0     1

    """

    return np.array([np.asanyarray(d).dtype.kind in NUMERIC_KINDS for d in np.nditer(np.asanyarray(dat))]).astype('int8')


def isreal(dat):
    """
    isreal - Check if input is real array
    Syntax
    TF = isreal(A)
    Description
    TF = isreal(A) returns logical 1 (true) if A does not have an imaginary part. It returns logical
    0 (false) otherwise. If A has a stored imaginary part of value 0, isreal(A) returns logical 0
    (false).
    Note   For logical and char data classes, isreal always returns true. For numeric data
    types, if A does not have an imaginary part isreal returns true; if A does have an imaginary
    part isreal returns false. For cell, struct, function_handle, and object data types,
    isreal always returns false.
    ~isreal(x) returns true for arrays that have at least one element with an imaginary
    component. The value of that component can be 0.
    Tips
    If A is real, complex(A) returns a complex number whose imaginary component is 0, and
    isreal(complex(A)) returns false. In contrast, the addition A + 0i returns the real value A,
    and isreal(A + 0i) returns true.
    If B is real and A = complex(B), then A is a complex matrix and isreal(A) returns false,
    while A(m:n) returns a real matrix and isreal(A(m:n)) returns true.
    Because MATLAB software supports complex arithmetic, certain of its functions can introduce
    significant imaginary components during the course of calculations that appear to be limited to
    real numbers. Thus, you should use isreal with discretion.
    Example 1
    If a computation results in a zero-value imaginary component, isreal returns true.
    x=3+4i;
    y=5-4i;
    isreal(x+y)
    ans =
         1
    Example 2
    These examples use isreal to detect the presence or absence of imaginary numbers in an
    array. Let
    x = magic(3);
    y = complex(x);
    isreal(x) returns true because no element of x has an imaginary component.
    isreal(x)
    ans =
         1
    isreal(y) returns false, because every element of x has an imaginary component, even
    though the value of the imaginary components is 0.
    isreal(y)
    ans =
         0
    This expression detects strictly real arrays, i.e., elements with 0-valued imaginary components
    are treated as real.
    ~any(imag(y(:)))
    ans =
         1
    Example 3
    Given the following cell array,
    C{1} = pi;                 % double
    C{2} = 'John Doe';         % char array
    C{3} = 2 + 4i;             % complex double
    C{4} = ispc;               % logical
    C{5} = magic(3);           % double array
    C{6} = complex(5,0)        % complex double
    C =
      [3.1416]  'John Doe'  [2.0000+ 4.0000i]  [1]  [3x3 double]  [5]
    isreal shows that all but C{1,3} and C{1,6} are real arrays.
    for k = 1:6
    x(k) = isreal(C{k});
    end
    x
    x =
         1     1     0     1     1    0
    """

    return np.array([np.asanyarray(d).dtype.kind in REAL_KINDS for d in np.nditer(np.asanyarray(dat))]).astype('int8')


def isscalar(dat):
    """
    isscalar - Determine whether input is scalar
    Syntax
    isscalar(A)
    Description
    isscalar(A) returns logical 1 (true) if size(A) returns [1 1], and logical 0 (false) otherwise.
    Examples
    Test matrix A and one element of the matrix:
    A = rand(5);
    isscalar(A)
    ans =
         0
    isscalar(A(3,2))
    ans =
         1


    """
    return np.asanyarray(dat).size == 1


def isvector(dat):
    """
    isvector - Determine whether input is vector
    Syntax
    isvector(A)
    Description
    isvector(A) returns logical 1 (true) if size(A) returns [1 n] or [n 1] with a nonnegative
    integer value n, and logical 0 (false) otherwise.
    Examples
    Test matrix A and its row and column vectors:
    A = rand(5);
    isvector(A)
    ans =
         0
    isvector(A(3, :))
    ans =
         1
    isvector(A(:, 2))
    ans =
         1


    """
    return np.asanyarray(dat).size > 1


def isempty(dat):
    """
    isempty - Test if array is empty
    Syntax
    tf = isempty(A)
    Description
    tf = isempty(A) returns logical true (1) if A is an empty array and logical false (0) otherwise.
    An empty array has at least one dimension of size zero, for example, 0-by-0 or 0-by-5.
    Examples
    B = rand(2,2,2);
    B(:,:,:) = [];
    isempty(B)
    ans =
         1
    """

    return np.asanyarray(dat).size == 0


def dataqc_globalrangetest(dat, datlim):
    """
    Global Range Quality Control Algorithm as defined in the DPS for SPEC_GLBLRNG - DCN 1341-10004
    https://alfresco.oceanobservatories.org/alfresco/d/d/workspace/SpacesStore/466c4915-c777-429a-8946-c90a8f0945b0/1341-10004_Data_Product_SPEC_GLBLRNG_OOI.pdf

    % DATAQC_GLOBALRANGETEST   Data quality control algorithm testing
    %      if measurements fall into a user-defined valid range.
    %      Returns 1 for presumably good data and 0 for data presumed bad.
    %
    % Time-stamp: <2010-07-28 15:16:00 mlankhorst>
    %
    % USAGE:   out=dataqc_globalrangetest(dat,validrange);
    %
    %          out: Boolean, 0 if value is outside range, else 1.
    %          dat: Input dataset, any scalar, vector, or matrix.
    %               Must be numeric and real.
    %          validrange: Two-element vector with the minimum and
    %               maximum values considered to be valid
    %
    % EXAMPLE:
    %
    %     >> x=[17 16 17 18 25 19];
    %     >> qc=dataqc_globalrangetest(x,[10 20])
    %
    %     qc =
    %
    %          1     1     1     1     0     1
    %
    %
    function out=dataqc_globalrangetest(dat,datlim);

        if ~isnumeric(dat)
            error('DAT must be numeric.')
          end
        if ~all(isreal(dat(:)))
            error('DAT must be real.')
          end
        if ~isnumeric(datlim)
            error('VALIDRANGE must be numeric.')
          end
        if ~all(isreal(datlim(:)))
            error('VALIDRANGE must be real.')
          end
        if length(datlim)~=2
            error('VALIDRANGE must be two-element vector.')
          end
        datlim=[min(datlim(:)) max(datlim(:))];
        out=(dat>=datlim(1))&(dat<=datlim(2))

    """

    dat_arr = np.asanyarray(dat)
    datlim_arr = np.asanyarray(datlim)

    if not isnumeric(dat_arr).all():
        raise ValueError('\'dat\' must be numeric')

    if not isreal(dat_arr).all():
        raise ValueError('\'dat\' must be real')

    if not isnumeric(datlim_arr).all():
        raise ValueError('\'datlim\' must be numeric')

    if not isreal(datlim_arr).all():
        raise ValueError('\'datlim\' must be real')

    if len(datlim_arr) < 2:  # Must have at least 2 elements
        raise ValueError('\'datlim\' must have at least 2 elements')

    return (datlim_arr.min() <= dat) & (dat <= datlim_arr.max()).astype('int8')


def dataqc_spiketest(dat, acc, N=5, L=5):
    """
    Spike Test Quality Control Algorithm as defined in the DPS for SPEC_SPKETST - DCN 1341-10006
    https://alfresco.oceanobservatories.org/alfresco/d/d/workspace/SpacesStore/eadad62c-ec80-403d-b3d3-c32c79f9e9e4/1341-10006_Data_Product_SPEC_SPKETST_OOI.pdf

    % DATAQC_SPIKETEST   Data quality control algorithm testing a time
    %                    series for spikes. Returns 1 for presumably
    %                    good data and 0 for data presumed bad.
    %
    % Time-stamp: <2010-07-28 14:25:42 mlankhorst>
    %
    % METHODOLOGY: The time series is divided into windows of length L
    %   (an odd integer number). Then, window by window, each value is
    %   compared to its (L-1) neighboring values: a range R of these
    %   (L-1) values is computed (max. minus min.), and replaced with
    %   the measurement accuracy ACC if ACC>R. A value is presumed to
    %   be good, i.e. no spike, if it deviates from the mean of the
    %   (L-1) peers by less than a multiple of the range, N*max(R,ACC).
    %
    %   Further than (L-1)/2 values from the start or end points, the
    %   peer values are symmetrically before and after the test
    %   value. Within that range of the start and end, the peers are
    %   the first/last L values (without the test value itself).
    %
    %   The purpose of ACC is to restrict spike detection to deviations
    %   exceeding a minimum threshold value (N*ACC) even if the data
    %   have little variability. Use ACC=0 to disable this behavior.
    %
    %
    % USAGE:   out=dataqc_spiketest(dat,acc,N,L);
    %    OR:   out=dataqc_spiketest(dat,acc);
    %
    %          out: Boolean. 0 for detected spike, else 1.
    %          dat: Input dataset, a real numeric vector.
    %          acc: Accuracy of any input measurement.
    %          N (optional, defaults to 5): Range multiplier, cf. above
    %          L (optional, defaults to 5): Window length, cf. above
    %
    % EXAMPLE:
    %
    %    >> x=[-4     3    40    -1     1    -6    -6     1];
    %    >> dataqc_spiketest(x,.1)
    %
    %    ans =
    %
    %         1     1     0     1     1     1     1     1
    %
    function out=dataqc_spiketest(varargin);

    error(nargchk(2,4,nargin,'struct'))
    dat=varargin{1};
    acc=varargin{2};
    N=5;
    L=5;
    switch nargin
        case 3,
            if ~isempty(varargin{3})Data Product Specification for Spike Test
                Ver 1-01 1341-10006 Appendix Page A-2
                N=varargin{3};
            end
        case 4,
            if ~isempty(varargin{3})
                N=varargin{3};
            end
            if ~isempty(varargin{4})
                L=varargin{4};
            end
    end
    if ~isnumeric(dat)
        error('DAT must be numeric.')
    end
    if ~isvector(dat)
        error('DAT must be a vector.')
    end
    if ~isreal(dat)
        error('DAT must be real.')
    end
    if ~isnumeric(acc)
        error('ACC must be numeric.')
    end
    if ~isscalar(acc)
        error('ACC must be scalar.')
    end
    if ~isreal(acc)
        error('ACC must be real.')
    end
    if ~isnumeric(N)
        error('N must be numeric.')
    end
    if ~isscalar(N)
        error('N must be scalar.')
    end
    if ~isreal(N)
        error('N must be real.')
    end
    if ~isnumeric(L)
        error('L must be numeric.')
    end
    if ~isscalar(L)
        error('L must be scalar.')
    end
    if ~isreal(L)
        error('L must be real.')
    end
    L=ceil(abs(L));
    if (L/2)==round(L/2)
        L=L+1;
        warning('L was even; setting L:=L+1')
    end
    if L<3
        L=5;
        warning('L was too small; setting L:=5')
    end
    ll=length(dat);

    L2=(L-1)/2;
    i1=1+L2;
    i2=ll-L2;

    if ll>=L

        for ii=i1:i2
            tmpdat=dat(ii+[-L2:-1 1:L2]);
            R=max(tmpdat)-min(tmpdat);
            R=max([R acc]);
            if (N*R)>abs(dat(ii)-mean(tmpdat))
                out(ii)=1;
            end
        end
        for ii=1:L2
            tmpdat=dat([1:ii-1 ii+1:L]);
            R=max(tmpdat)-min(tmpdat);
            R=max([R acc]);
            if (N*R)>abs(dat(ii)-mean(tmpdat))
                out(ii)=1;
            end
        end
        for ii=ll-L2+1:ll
            tmpdat=dat([ll-L+1:ii-1 ii+1:ll]);
            R=max(tmpdat)-min(tmpdat);
            R=max([R acc]);
            if (N*R)>abs(dat(ii)-mean(tmpdat))
                out(ii)=1;
            end
        end
    else
        warning('L was greater than length of DAT, returning zeros.')
    end

    """

    dat_arr = np.asanyarray(dat)

    if not isnumeric(dat_arr).all():
        raise ValueError('\'dat\' must be numeric')

    if not isvector(dat_arr):
        raise ValueError('\'dat\' must be a vector')

    if not isreal(dat_arr).all():
        raise ValueError('\'dat\' must be real')

    for k, arg in {'acc': acc, 'N': N, 'L': L}.iteritems():
        if not isnumeric(arg).all():
            raise ValueError('\'{0}\' must be numeric'.format(k))

        if not isscalar(arg):
            raise ValueError('\'{0}\' must be a scalar'.format(k))

        if not isreal(arg).all():
            raise ValueError('\'{0}\' must be real'.format(k))

    L = np.ceil(np.abs(L))
    if L / 2 == np.round(L / 2):
        L += 1
        # Warn - L was even; setting L = L + 1
    if L < 3:
        L = 5
        # Warn - L was too small; setting L = 5

    ll = len(dat_arr)
    out = np.zeros(dat_arr.size, dtype='int8')

    L2 = int((L - 1) / 2)
    i1 = 1 + L2
    i2 = ll - L2

    if ll >= L:

        for ii in xrange(i1 - 1,i2):  # for ii=i1:i2
            tmpdat = np.hstack((dat_arr[ii - L2:ii], dat_arr[ii + 1:ii + 1 + L2]))  # tmpdat=dat(ii+[-L2:-1 1:L2]);
            R = tmpdat.max() - tmpdat.min()  # R=max(tmpdat)-min(tmpdat);
            R = np.max([R, acc])  # R=max([R acc]);
            if (N * R) > np.abs(dat_arr[ii] - tmpdat.mean()):  # if (N*R)>abs(dat(ii)-mean(tmpdat))
                out[ii] = 1  # out(ii)=1;

        for ii in xrange(L2):  # for ii=1:L2
            tmpdat = np.hstack((dat_arr[:ii], dat_arr[ii+1:L]))  # tmpdat=dat([1:ii-1 ii+1:L]);
            R = tmpdat.max() - tmpdat.min()  # R=max(tmpdat)-min(tmpdat);
            R = np.max([R, acc])  # R=max([R acc]);
            if (N * R) > np.abs(dat_arr[ii] - tmpdat.mean()):  # if (N*R)>abs(dat(ii)-mean(tmpdat))
                out[ii] = 1  # out(ii)=1;

        for ii in xrange(ll - L2, ll):  # for ii=ll-L2+1:ll
            tmpdat = np.hstack((dat_arr[:ii], dat_arr[ii:L]))  # tmpdat=dat([ll-L+1:ii-1 ii+1:ll]);
            R = tmpdat.max() - tmpdat.min()  # R=max(tmpdat)-min(tmpdat);
            R = np.max([R, acc])  # R=max([R acc]);
            if (N * R) > np.abs(dat_arr[ii] - tmpdat.mean()):  # if (N*R)>abs(dat(ii)-mean(tmpdat))
                out[ii] = 1  # out(ii)=1;

    else:
        pass
        # Warn - 'L was greater than length of DAT, returning zeros.'

    return out


def dataqc_stuckvaluetest(x, reso, num=10):
    """
    Stuck Value Test Quality Control Algorithm as defined in the DPS for SPEC_STUCKVL - DCN 1341-10008
    https://alfresco.oceanobservatories.org/alfresco/d/d/workspace/SpacesStore/a04acb56-7e27-48c6-a40b-9bb9374ee35c/1341-10008_Data_Product_SPEC_STUCKVL_OOI.pdf

    % DATAQC_STUCKVALUETEST   Data quality control algorithm testing a
    %     time series for "stuck values", i.e. repeated occurences of
    %     one value. Returns 1 for presumably good data and 0 for data
    %     presumed bad.
    %
    % Time-stamp: <2011-10-31 11:20:23 mlankhorst>
    %
    % USAGE:   OUT=dataqc_stuckvaluetest(X,RESO,NUM);
    %
    %       OUT:  Boolean output: 0 where stuck values are found,
    %             1 elsewhere.
    %       X:    Input time series (vector, numeric).
    %       RESO: Resolution; repeat values less than RESO apart will
    %             be considered "stuck values".
    %       NUM:  Minimum number of successive values within RESO of
    %             each other that will trigger the "stuck value". NUM
    %             is optional and defaults to 10 if omitted or empty.
    %
    % EXAMPLE:
    %
    % >> x=[4.83  1.40  3.33  3.33  3.33  3.33  4.09  2.97  2.85  3.67];
    %
    % >> dataqc_stuckvaluetest(x,.001,4)
    %
    % ans =
    %
    %       1     1     0     0     0     0     1     1     1     1
    %
    function out=dataqc_stuckvaluetest(varargin);

    error(nargchk(2,3,nargin,'struct'))
    x=varargin{1};
    reso=varargin{2};
    num=10;
    switch nargin
        case 3,
            if ~isempty(varargin{3})
                num=varargin{3};
            end
    end
    if ~isnumeric(x)
        error('X must be numeric.')
    end
    if ~isvector(x)
        error('X must be a vector.')
    end
    if ~isnumeric(reso)
        error('RESO must be numeric.')
    end
    if ~isscalar(reso)
        error('RESO must be a scalar.')
    end
    if ~isreal(reso)
        error('RESO must be real.')
    end
    reso=abs(reso);
    if ~isnumeric(num)
        error('NUM must be numeric.')
    end
    if ~isscalar(num)
        error('NUM must be a scalar.')
    end
    if ~isreal(num)
        error('NUM must be real.')
    end
    num=abs(num);
    ll=length(x);
    out=zeros(size(x));
    out=logical(out);
    if ll<num
        warning('NUM is greater than length(X). Returning zeros.')
    else
        out=ones(size(x));
        iimax=ll-num+1;
        for ii=1:iimax
            ind=[ii:ii+num-1];
            tmp=abs(x(ii)-x(ind));
            if all(tmp<reso)
                out(ind)=0;
            end
        end
    end
    out=logical(out);
    """

    dat_arr = np.asanyarray(x)

    if not isnumeric(dat_arr).all():
        raise ValueError('\'x\' must be numeric')

    if not isvector(dat_arr):
        raise ValueError('\'x\' must be a vector')

    if not isreal(dat_arr).all():
        raise ValueError('\'x\' must be real')

    for k, arg in {'reso': reso, 'num': num}.iteritems():
        if not isnumeric(arg).all():
            raise ValueError('\'{0}\' must be numeric'.format(k))

        if not isscalar(arg):
            raise ValueError('\'{0}\' must be a scalar'.format(k))

        if not isreal(arg).all():
            raise ValueError('\'{0}\' must be real'.format(k))

    num = np.abs(num)
    ll = len(x)
    out = np.zeros(dat_arr.size, dtype='int8')

    if ll < num:
        # Warn - 'num' is greater than length(x), returning zeros
        pass
    else:
        out.fill(1)
        iimax = ll - num+1
        for ii in xrange(iimax):
            slice_ = slice(ii, ii + num)
            tmp = np.abs(dat_arr[ii] - dat_arr[slice_])
            if (tmp < reso).all():
                out[slice_] = 0

    return out



