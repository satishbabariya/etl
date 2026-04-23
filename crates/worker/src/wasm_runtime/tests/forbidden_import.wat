(component
  (core module $m
    (import "forbidden" "wall_clock_now" (func (result i64)))
    (func (export "run") (result i64)
      (call 0)
    )
  )
  (core instance $i (instantiate $m))
  (func (export "go") (result s64) (canon lift (core func $i "run")))
)
