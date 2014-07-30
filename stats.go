package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"time"
)

type Stat struct {
	name  string
	value int
}

var chstatcounter chan Stat

func init() {
	chstatcounter = make(chan Stat, 200)
}

// Show statistics about the program every `frequency` seconds
// Counters be created and incremented by sending a string with the name of the
// counter to channel `chstatcounter`
func stats(frequency int, memory bool) {
	counters := sort.StringSlice{}
	counter_values := make(map[string]int)
	lock := &sync.Mutex{}

	// Increment counter statistics
	go func() {
		for c := range chstatcounter {
			lock.Lock()
			if val, ok := counter_values[c.name]; ok {
				counter_values[c.name] = val + c.value
			} else {
				counters = append(counters, c.name)
				counters.Sort()

				counter_values[c.name] = c.value
			}
			lock.Unlock()
		}
	}()

	// Display stats at each given interval
	go func() {
		m := runtime.MemStats{}
		last := time.Now()
		diff := 0
		w := bufio.NewWriter(os.Stdout)

		// Values from last print
		last_values := make(map[string]int)
		timer := time.NewTimer(time.Duration(0))

		for t := range timer.C {
			lock.Lock()

			// Time difference since last call
			diff = int(t.Sub(last) / time.Second)
			last = t

			runtime.ReadMemStats(&m)

			fmt.Fprintf(w, t.Format("2006/01/02 15:04:05 "))

			// Counters
			for _, c := range counters {
				val := counter_values[c]
				val_last := last_values[c]

				fmt.Fprintf(w, "%s: %d (%d", c, val, val-val_last)

				if diff != 0 {
					fmt.Fprintf(w, " %d/s", (val-val_last)/diff)
				}
				w.WriteString(")\t")

				last_values[c] = val
			}

			if memory {
				fmt.Fprintf(w, " mem: %d sys %d alloc %d idle %d released",
					m.HeapSys, m.HeapAlloc, m.HeapIdle, m.HeapReleased)
			}

			w.WriteRune('\n')
			w.Flush()

			lock.Unlock()

			timer.Reset(time.Duration(frequency) * time.Second)
		}
	}()
}

// Periodically save heap stats
func UpdateHeapProfile() {
	timer := time.NewTimer(time.Duration(0))

	for _ = range timer.C {
		pprof.WriteHeapProfile(fheap)
		timer.Reset(time.Second)
	}
}
