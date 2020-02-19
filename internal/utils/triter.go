package utils

type TimeRange struct {
	Start, End int64
}

type TimeRangeIterator interface {
	Next() bool
	At() TimeRange
}

func NewTimeRangeIter(mint, maxt, step int64, backward bool) TimeRangeIterator {
	if !backward {
		return newForwardIter(mint, maxt, step)
	}
	return newBackwardIter(mint, maxt, step)
}

type timeRangeForwardIterator struct {
	mint, maxt, step int64
	r                TimeRange
}

func newForwardIter(mint, maxt, step int64) *timeRangeForwardIterator {
	return &timeRangeForwardIterator{mint: mint, maxt: maxt, step: step}
}

func (i *timeRangeForwardIterator) Next() bool {
	if i.r.End == i.maxt {
		return false
	}
	if i.r.Start == 0 && i.r.End == 0 {
		i.r.Start = i.mint
		i.r.End = i.mint + i.step
	} else {
		i.r.Start = i.r.End
		i.r.End += i.step
	}
	if i.r.End > i.maxt {
		i.r.End = i.maxt
	}
	return true
}

func (i *timeRangeForwardIterator) At() TimeRange {
	return i.r
}

type timeRangeBackwardIterator struct {
	mint, maxt, step int64
	r                TimeRange
}

func newBackwardIter(mint, maxt, step int64) *timeRangeBackwardIterator {
	return &timeRangeBackwardIterator{mint: mint, maxt: maxt, step: step}
}

func (i *timeRangeBackwardIterator) Next() bool {
	if i.r.Start == i.mint {
		return false
	}
	if i.r.Start == 0 && i.r.End == 0 {
		i.r.End = i.maxt
		i.r.Start = i.maxt - i.step
	} else {
		i.r.End = i.r.Start
		i.r.Start = i.r.Start - i.step
	}
	if i.r.Start < i.mint {
		i.r.Start = i.mint
	}
	return true
}

func (i *timeRangeBackwardIterator) At() TimeRange {
	return i.r
}
