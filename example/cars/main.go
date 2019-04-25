package main

import (
	"fmt"
	"os"

	"github.com/cbhvn/gkvlite"
)

func checkerr(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	path := "test.gkvlite"
	f, err := os.Create(path)
	checkerr(err)
	s, err := gkvlite.NewStore(f)
	checkerr(err)
	c := s.SetCollection("cars", nil)

	// You can also retrieve the collection, where c == cc.
	_ = s.GetCollection("cars")

	// Insert values.
	c.Set([]byte("tesla"), []byte("$$$"))
	c.Set([]byte("mercedes"), []byte("$$"))
	c.Set([]byte("bmw"), []byte("$"))

	// Replace values.
	c.Set([]byte("tesla"), []byte("$$$$"))

	// Retrieve values.
	mercedesPrice, err := c.Get([]byte("mercedes"))
	checkerr(err)
	fmt.Println("mercedesPrice", string(mercedesPrice))

	// One of the most priceless vehicles is not in the collection.
	thisIsNil, err := c.Get([]byte("the-apollo-15-moon-buggy"))
	checkerr(err)
	fmt.Println("thisIsNil", thisIsNil == nil)
	// Iterate through items.
	c.VisitItemsAscend([]byte("ford"), true, func(i *gkvlite.Item) bool {
		// This visitor callback will be invoked with every item
		// with key "ford" and onwards, in key-sorted order.
		// So: "mercedes", "tesla" are visited, in that ascending order,
		// but not "bmw".
		// If we want to stop visiting, return false;
		// otherwise return true to keep visiting.
		return true
	})

	// Let's get a snapshot.
	snap := s.Snapshot()
	snapCars := snap.GetCollection("cars")

	// The snapshot won't see modifications against the original Store.
	c.Delete([]byte("mercedes"))
	mercedesIsNil, err := c.Get([]byte("mercedes"))
	checkerr(err)
	fmt.Println("mercedesIsNil", mercedesIsNil == nil)
	mercedesPriceFromSnapshot, err := snapCars.Get([]byte("mercedes"))
	checkerr(err)
	fmt.Println("mercedesPriceFromSnapshot", string(mercedesPriceFromSnapshot))
	// Persist all the changes to disk.
	s.Flush()

	f.Sync() // Some applications may also want to fsync the underlying file.

	// Now, other file readers can see the data, too.
	f2, err := os.Open(path)
	checkerr(err)
	s2, err := gkvlite.NewStore(f2)
	checkerr(err)
	c2 := s2.GetCollection("cars")

	bmwPrice, err := c2.Get([]byte("bmw"))
	checkerr(err)
	fmt.Println("bmwPrice", string(bmwPrice))
}
