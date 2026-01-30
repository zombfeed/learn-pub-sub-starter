package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(state routing.PlayingState) {
		fmt.Println("handlerPause called, state:", state)
		defer fmt.Print("> ")
		gs.HandlePause(state)
	}
}
