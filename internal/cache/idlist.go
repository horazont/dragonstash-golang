package cache

import "errors"

type idSegment struct {
	start uint64
	end   uint64
}

type IDList struct {
	segments []idSegment
	total    uint64
}

func NewEmptyIDList() (result *IDList) {
	result = new(IDList)
	// we can allocate the first segment right away, it will most likely be
	// used
	result.segments = make([]idSegment, 0, 1)
	result.total = 0
	return result
}

func (m *IDList) findSegment(id uint64) int {
	for i := range m.segments {
		segment := &m.segments[i]
		if segment.start >= id {
			return i
		}
	}
	return len(m.segments)
}

func (m *IDList) extendSegment(segment_index int) {
	segment1 := &m.segments[segment_index]
	segment2 := &m.segments[segment_index+1]
	if segment1.end+1 != segment2.start {
		panic("extendSegment can only work with consecutive segments")
	}

	segment1.end = segment2.end
	m.segments = append(
		m.segments[:segment_index+1],
		m.segments[segment_index+2:]...,
	)
}

func (m *IDList) Count() uint64 {
	return m.total
}

func (m *IDList) Alloc() (uint64, error) {
	if len(m.segments) == 0 {
		return 0, errors.New("out of IDs")
	}
	segment := &m.segments[0]
	result := segment.start
	segment.start += 1
	m.total -= 1
	if segment.start > segment.end {
		m.segments = m.segments[1:]
	}
	return result, nil
}

func (m *IDList) AddBlock(start uint64, end uint64) error {
	segment_index := m.findSegment(start)
	count := (end - start) + 1

	if segment_index > 0 {
		prev_segment := &m.segments[segment_index-1]
		if prev_segment.end >= start {
			return errors.New("new block overlaps existing block")
		}

		if prev_segment.end+1 == start {
			prev_segment.end = end
			m.total += count
			if segment_index < len(m.segments) {
				segment := &m.segments[segment_index]
				if segment.start == end+1 {
					m.extendSegment(segment_index - 1)
				}
			}
			return nil
		}
	}

	if segment_index < len(m.segments) {
		segment := &m.segments[segment_index]
		if segment.start <= end {
			return errors.New("new block overlaps existing block")
		}

		if segment.start == end+1 {
			segment.start = start
			m.total += count
			return nil
		}

		m.segments = append(m.segments[:segment_index],
			append([]idSegment{idSegment{start, end}},
				m.segments[segment_index:]...,
			)...,
		)
	} else {
		m.segments = append(m.segments, idSegment{start, end})
	}
	m.total += count

	return nil
}

func (m *IDList) Release(id uint64) {
	err := m.AddBlock(id, id)
	if err != nil {
		panic(err.Error())
	}
}
