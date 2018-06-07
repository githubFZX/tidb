// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"github.com/pingcap/tidb/ast"
	log "github.com/sirupsen/logrus"
)

// specialFoldHandler stores functions for special UDF to constant fold
var specialFoldHandler = map[string]func(*ScalarFunction) (Expression, bool){}

func init() {
	specialFoldHandler = map[string]func(*ScalarFunction) (Expression, bool){
		ast.If:     ifFoldHandler,
		ast.Ifnull: ifNullFoldHandler,
	}
}

// FoldConstant does constant folding optimization on an expression excluding deferred ones.
func FoldConstant(expr Expression) Expression {
	e, _ := foldConstant(expr)
	return e
}

func ifFoldHandler(expr *ScalarFunction) (Expression, bool) {
	args := expr.GetArgs()
	foldedArg0, _ := foldConstant(args[0])
	if constArg, isConst := foldedArg0.(*Constant); isConst {
		arg0, isNull0, err := constArg.EvalInt(expr.Function.getCtx(), nil)
		if err != nil {
			log.Warnf("fold constant %s: %s", expr.ExplainInfo(), err.Error())
			return expr, false
		}
		if !isNull0 && arg0 != 0 {
			return foldConstant(args[1])
		}
		return foldConstant(args[2])
	}
	var isDeferred, isDeferredConst bool
	expr.GetArgs()[1], isDeferred = foldConstant(args[1])
	isDeferredConst = isDeferredConst || isDeferred
	expr.GetArgs()[2], isDeferred = foldConstant(args[2])
	isDeferredConst = isDeferredConst || isDeferred
	return expr, isDeferredConst
}

func ifNullFoldHandler(expr *ScalarFunction) (Expression, bool) {
	args := expr.GetArgs()
	foldedArg0, _ := foldConstant(args[0])
	if constArg, isConst := foldedArg0.(*Constant); isConst {
		_, isNull0, err := constArg.EvalInt(expr.Function.getCtx(), nil)
		if err != nil {
			log.Warnf("fold constant %s: %s", expr.ExplainInfo(), err.Error())
			return expr, false
		}
		if isNull0 == true {
			return foldConstant(args[1])
		}
	}
	isDeferredConst := false
	expr.GetArgs()[1], isDeferredConst = foldConstant(args[1])
	return expr, isDeferredConst
}

func foldConstant(expr Expression) (Expression, bool) {
	switch x := expr.(type) {
	case *ScalarFunction:
		if _, ok := unFoldableFunctions[x.FuncName.L]; ok {
			return expr, false
		}
		if function := specialFoldHandler[x.FuncName.L]; function != nil {
			return function(x)
		}

		args := x.GetArgs()
		canFold := true
		isDeferredConst := false
		for i := 0; i < len(args); i++ {
			foldedArg, isDeferred := foldConstant(args[i])
			x.GetArgs()[i] = foldedArg
			_, conOK := foldedArg.(*Constant)
			if !conOK {
				canFold = false
			}
			isDeferredConst = isDeferredConst || isDeferred
		}
		if !canFold {
			return expr, isDeferredConst
		}
		value, err := x.Eval(nil)
		if err != nil {
			log.Warnf("fold constant %s: %s", x.ExplainInfo(), err.Error())
			return expr, isDeferredConst
		}
		if isDeferredConst {
			return &Constant{Value: value, RetType: x.RetType, DeferredExpr: x}, true
		}
		return &Constant{Value: value, RetType: x.RetType}, false
	case *Constant:
		if x.DeferredExpr != nil {
			value, err := x.DeferredExpr.Eval(nil)
			if err != nil {
				log.Warnf("fold constant %s: %s", x.ExplainInfo(), err.Error())
				return expr, true
			}
			return &Constant{Value: value, RetType: x.RetType, DeferredExpr: x.DeferredExpr}, true
		}
	}
	return expr, false
}
