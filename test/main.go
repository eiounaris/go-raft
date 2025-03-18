package main

// === flag

// func main() {
// 	// 定义标志
// 	name := flag.String("name", "默认名", "用户名称")
// 	age := flag.Int("age", 0, "用户年龄")

// 	// 解析命令行参数
// 	flag.Parse()

// 	// 使用解析后的值
// 	fmt.Printf("姓名: %s, 年龄: %d\n", *name, *age)
// }

// === github.com/dgraph-io/badger/v4

import (
	"fmt"
	"log"

	"slices"

	badger "github.com/dgraph-io/badger/v4"
)

func main() {
	// Open the Badger database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	logdb, err := badger.Open(badger.DefaultOptions("data/logdb"))
	if err != nil {
		log.Fatal(err)
	}

	defer logdb.Close()

	// Open the Badger database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	kvdb, err := badger.Open(badger.DefaultOptions("data/kvdb"))
	if err != nil {
		log.Fatal(err)
	}

	defer kvdb.Close()

	// your code here
	err = logdb.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte("logdb"), []byte("logdb"))
		return err
	})
	if err != nil {
		log.Println(err)
	}

	err = logdb.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("logdb"))
		if err != nil {
			return err
		}

		var valCopy []byte
		// var valNot []byte
		err = item.Value(func(val []byte) error {
			// This func with val would only be called if item.Value encounters no error.

			// Accessing val here is valid.
			fmt.Printf("The logdb is: %s\n", val)

			// Copying or parsing val is valid.
			valCopy = slices.Clone(val)

			// Assigning val slice to another variable is NOT OK.
			// valNot = val // Do not do this.
			return nil
		})
		if err != nil {
			return err
		}

		// DO NOT access val here. It is the most common cause of bugs.
		// fmt.Printf("NEVER do this. %s\n", valNot)

		// You must copy it to use it outside item.Value(...).
		fmt.Printf("The logdb is: %s\n", valCopy)

		// Alternatively, you could also use item.ValueCopy().
		valCopy, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		fmt.Printf("The logdb is: %s\n", valCopy)

		return nil
	})
	if err != nil {
		log.Println(err)
	}

	err = logdb.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte("logdb"))
		return err
	})
	if err != nil {
		log.Println(err)
	}

	err = logdb.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("logdb"))
		if err != nil {
			return err
		}

		var valCopy []byte
		// var valNot []byte
		err = item.Value(func(val []byte) error {
			// This func with val would only be called if item.Value encounters no error.

			// Accessing val here is valid.
			fmt.Printf("The logdb is: %s\n", val)

			// Copying or parsing val is valid.
			valCopy = slices.Clone(val)

			// Assigning val slice to another variable is NOT OK.
			// valNot = val // Do not do this.
			return nil
		})
		if err != nil {
			return err
		}

		// DO NOT access val here. It is the most common cause of bugs.
		// fmt.Printf("NEVER do this. %s\n", valNot)

		// You must copy it to use it outside item.Value(...).
		fmt.Printf("The logdb is: %s\n", valCopy)

		// Alternatively, you could also use item.ValueCopy().
		valCopy, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		fmt.Printf("The logdb is: %s\n", valCopy)

		return nil
	})
	if err != nil {
		log.Println(err)
	}

	// your code here
	err = kvdb.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte("kvdb"), []byte("kvdb"))
		return err
	})
	if err != nil {
		log.Println(err)
	}

	err = kvdb.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("kvdb"))
		if err != nil {
			return err
		}

		var valCopy []byte
		// var valNot []byte
		err = item.Value(func(val []byte) error {
			// This func with val would only be called if item.Value encounters no error.

			// Accessing val here is valid.
			fmt.Printf("The kvdb is: %s\n", val)

			// Copying or parsing val is valid.
			valCopy = slices.Clone(val)

			// Assigning val slice to another variable is NOT OK.
			// valNot = val // Do not do this.
			return nil
		})
		if err != nil {
			return err
		}

		// DO NOT access val here. It is the most common cause of bugs.
		// fmt.Printf("NEVER do this. %s\n", valNot)

		// You must copy it to use it outside item.Value(...).
		fmt.Printf("The kvdb is: %s\n", valCopy)

		// Alternatively, you could also use item.ValueCopy().
		valCopy, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		fmt.Printf("The kvdb is: %s\n", valCopy)

		return nil
	})
	if err != nil {
		log.Println(err)
	}

	err = kvdb.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte("kvdb"))
		return err
	})
	if err != nil {
		log.Println(err)
	}

	err = kvdb.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("kvdb"))
		if err != nil {
			return err
		}

		var valCopy []byte
		// var valNot []byte
		err = item.Value(func(val []byte) error {
			// This func with val would only be called if item.Value encounters no error.

			// Accessing val here is valid.
			fmt.Printf("The kvdb is: %s\n", val)

			// Copying or parsing val is valid.
			valCopy = slices.Clone(val)

			// Assigning val slice to another variable is NOT OK.
			// valNot = val // Do not do this.
			return nil
		})
		if err != nil {
			return err
		}

		// DO NOT access val here. It is the most common cause of bugs.
		// fmt.Printf("NEVER do this. %s\n", valNot)

		// You must copy it to use it outside item.Value(...).
		fmt.Printf("The kvdb is: %s\n", valCopy)

		// Alternatively, you could also use item.ValueCopy().
		valCopy, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		fmt.Printf("The kvdb is: %s\n", valCopy)

		return nil
	})
	if err != nil {
		log.Println(err)
	}
}
