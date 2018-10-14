package main

import "fmt"
import "time"

// return list of primes less than N
func sieveOfEratosthenes(N int) (primes []int) {
	b := make([]bool, N)
	for i := 2; i < N; i++ {
		time.Sleep(1 * time.Second)
		if b[i] == true {
			continue
		}
		primes = append(primes, i)
		for k := i * i; k < N; k += i {
			b[k] = true
		}
	}
	return
}

func main() {
	primes := sieveOfEratosthenes(100)
	for _, p := range primes {
		fmt.Printf("%v ", p)
	}
}
