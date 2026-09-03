
# Functions Column Modality {.dqlh}


Any syntax of the form `foo:(x,y)` requires
that its columns be existent and passed in:

```delightql
 users(*) |> ( upper:(last_name), upper:("literal") )
```

The above syntax might seem almost counter-intuitively obvious to
anyone who has used a general-purpose programming.

But delightql allows other syntax that is *still* input-moded
and which may be unfamiliar to those not used to Prolog:

```delightql
descendant(parent, "Isaac"), upper:(parent)="ABRAHAM"
```

Nevertheless, the fact that a form `foo:(a)` is discernible
at all means that this column **must** be input only.

