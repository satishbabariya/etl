(component
  (core module $m
    (memory (export "mem") 1)
    (func (export "grow_lots") (result i32)
      (memory.grow (i32.const 1024))
    )
  )
  (core instance $i (instantiate $m))
  (func (export "grow") (result s32) (canon lift (core func $i "grow_lots")))
)
