#!/usr/bin/env python3

#
# Interstate
# Copyright 2023 by Larry Hastings
#
# Simple, pythonic state machine manager.
#

import abc
import functools
import inspect


class State:
    """
    Base class for state machine state implementation classes.
    Use of this base class is optional; states can be instances
    of any type.
    """

    def on_enter(self):
        """
        Called when entering this state.  Optional.
        """
        pass

    def on_exit(self):
        """
        Called when exiting this state.  Optional.
        """
        pass


class TransitionError(RecursionError):
    """
    Raised when attempting to make an illegal state transition.

    There are only two types of illegal state transitions:

    * An attempted state transition while we're in the process
      of transitioning to another state.  (In other words,
      if i is an Interstate object, you can't set i.state
      when i.next is not None.)
    * An attempt to transition to the current state.
      This is illegal:
          i = Interstate()
          i.state = foo
          i.state = foo  # <-- this line raises TransitionError
    """
    pass



class Interstate:
    """
    Simple, Pythonic state machine manager.

    Has three attributes:

        'state' is the current state.  You transition to the
        next state by assigning to 'state'.

        'next' is the state we are transitioning to, if we're
        currently in the process of transitioning to a new state.
        If we aren't currently transitioning to a new state,
        'next' is None.

        'state_class' is either None or a class.  If it's a
        class, Interstate will check that every value assigned
        to 'state' is an instance of that class.

    The constructor takes two parameters.  The first parameter,
    'state', is the initial state of the state machine.
    The second parameter, 'state_class', is keyword-only.  It's
    set as the 'state_class' attribute mentioned above.


    To transition to a new state, assign to the 'state' attribute.

        You may use any value as a state, including None.

        If the current state object has an 'on_exit' attribute,
        it will be called as a function with zero arguments during
        the the transition to the next state.

        If you assign an object to 'state' that has an 'on_enter'
        attribute, it will be called as a function with zero
        arguments immediately after we have transitioned to that
        state.

        It's illegal to assign to 'state' while currently
        transitioning to a new state.  (Or, in other words,
        at any time self.next is not None.)

        It's illegal to attempt to transition to the current
        state.  (If self.state == foo, self.state = foo is an error.)

        If you have an Interstate instance called "interstate",
        and you assign "state" to it:

            interstate.state = newstate

        Interstate executes the following sequence of actions:

            * Sets self.next to 'newstate'.
            * At of this moment your Interstate instance is
              "transitioning" to the new state.
            * If self.state has an attribute called 'on_exit',
              calls self.state.on_exit().
            * Sets self.next to None.
            * Sets self.state to 'newstate'.
            * As of this moment the transition is complete and
              your Interstate instance is "in" in the new state.
            * If self.state has an attribute called 'on_enter',
              calls self.state.on_enter().

    """

    __transitioning = False

    __state = None
    @property
    def state(self):
        return self.__state

    def _ensure_state_transition_is_valid(self, state):
        if (self.state_class is not None) and (not isinstance(state, self.state_class)):
            raise TypeError(f"invalid state object {state}, must be an instance of {self.state_class.__name__}")
        if self.__transitioning:
            raise TransitionError(f"can't start new state transition to {state}, still processing transition to {self.__next}")
        # use of "is" is deliberate here.
        # Interstate doesn't permit transitioning to the same exact state *object.*
        # but you can transition to an *identical* object.
        # you may change something in on_exit/on_enter.
        # (which maybe is an argument for not doing the check at all.
        # but that debate is for another day.)
        #
        # Also, it means math.nan doesn't break our API contract ;-)
        if state is self.__state:
            raise TransitionError(f"can't transition to {state}, it's already the current state")

    def _state_changed(self):
        new_state = self.state
        for o in tuple(self.observers):
            on_state_changed = getattr(o, self.on_state_changed, None)
            assert on_state_changed
            on_state_changed(new_state)

    @state.setter
    def state(self, state):
        self._ensure_state_transition_is_valid(state)
        self.__next = state
        self.__transitioning = True
        # as of this moment we are transitioning to "state"
        if self.on_exit:
            on_exit = getattr(self.__state, self.on_exit, None)
            if on_exit is not None:
                on_exit()
        self.__state = state
        self.__next = None
        self.__transitioning = False
        # as of this moment we are in "state".
        # (it's explicitly permitted to start a new state transition from inside enter().)
        if self.on_enter:
            on_enter = getattr(self.__state, self.on_enter, None)
            if on_enter is not None:
                on_enter()
        self._state_changed()

    __next = None
    @property
    def next(self):
        return self.__next

    def __init__(self,
        state,
        *,
        observers=(),
        on_enter='on_enter',
        on_exit='on_exit',
        on_state_changed='on_state_changed',
        state_class=None,
        ):
        if not ((state_class is None) or isinstance(state_class, type)):
            raise TypeError(f"state_class value {state_class} is invalid, it must either be None or a class")
        self.state_class = state_class
        self.on_enter = on_enter
        self.on_exit = on_exit
        self.on_state_changed = on_state_changed
        self.observers = list(observers)
        self._ensure_state_transition_is_valid(state)
        self.__state = state
        if self.on_enter:
            on_enter = getattr(self.__state, self.on_enter, None)
            if on_enter is not None:
                on_enter()
        self._state_changed()

    def add_observer(self, observer):
        if observer in self.observers:
            raise ValueError(f"observer {observer=} already added")
        self.observers.append(observer)

    def remove_observer(self, observer):
        if observer not in self.observers:
            raise ValueError(f"observer {observer=} not added")
        self.observers.remove(observer)

def accessor(accessor='state', attribute='interstate'):
    """
    Adds a convenient state accessor attribute to your class.

    Example:

        @accessor()
        class StateMachine:
            def __init__(self):
                self.interstate = Interstate(self.InitialState)
        sm = StateMachine()

    This creates an attribute of StateMachine called "state".
    'state' behaves identically to the "state" attribute
    of the "interstate" attribute of StateMachine.  It evaluates
    to the same value:

        sm.state == sm.interstate.state

    And setting it sets the state on the Interstate object.
    These two statements now do the same thing:

        sm.interstate.state = new_state
        sm.state = new_state

    By default, this decorator assumes your Interstate instance
    is stored in 'interstate', and you want to name the new accessor
    attribute 'state'.  You can override these defaults; the first
    parameter, 'accessor', is the name that will be used for the
    new accessor attribute, and the second parameter, 'attribute',
    should be the name of the attribute where your Interstate instance
    is stored.
    """
    def _accessor(cls):
        def getter(self):
            i = getattr(self, attribute)
            return getattr(i, 'state')
        def setter(self, value):
            i = getattr(self, attribute)
            setattr(i, 'state', value)
        setattr(cls, accessor, property(getter, setter))
        return cls
    return _accessor

def dispatch(attribute='interstate', *, prefix='', suffix=''):
    """
    Decorator for a state machine event method,
    dispatching the event to the current state.

    If you have a state machine class "StateMachine",
    which contains an Interstate state manager called "sm",
    and you want "StateMachine" to dispatch the event
    "on_sunrise" to the current state, this decorator
    can handle the boilerplate for you.  Instead of writing
    this:

        class StateMachine:
            def __init__(self):
                self.interstate = Interstate(self.InitialState)

            def on_sunrise(self, time, *, verbose=False):
                return self.interstate.state.on_sunrise(time, verbose=verbose)

    you can literally write this:

        class StateMachine:
            def __init__(self):
                self.interstate = Interstate(self.InitialState)

            @dispatch()
            def on_sunrise(self, time, *, verbose=False):
                ...

    Yes, with the ellipsis.  The existing on_sunrise function
    is ignored.  It's replaced with a function that gets
    the "interstate" attribute from self, then gets the 'state'
    attribute from the Interstate instance, then calls a method
    with the same name as the decorated function,
    passing in *args and **kwargs.

    attribute is the name of the attribute where the Interstate
    instance is stored in self.  The default is 'interstate'.

    prefix and suffix are strings added to the beginning and end
    of the method call we call on the current state.  For example,
    if you want the method you call to have an active verb form
    (e.g. 'reset'), but you want it to directly call an event
    handler that starts with 'on_' by convention (e.g. 'on_reset'),
    you could do this:

        @dispatch('smedley', prefix='on_')
        def reset(self):
            ...

    This would be equivalent to writing:

        def reset(self):
            return self.smedley.state.on_reset()
    """
    def dispatch(fn):
        def wrapper(self, *args, **kwargs):
            i = getattr(self, attribute)
            method = getattr(i.state, f'{prefix}{fn.__name__}{suffix}')
            return method(*args, **kwargs)
        functools.update_wrapper(wrapper, fn)
        wrapper.__signature__ = inspect.signature(fn)
        return wrapper
    return dispatch

def pure_virtual():
    """
    Decorator for pure virtual methods of a state
    base class.  Calling a method decorated with this
    raises a NotImplementedError exception.

    Best practice with Interstate is to use child
    classes to implement the states of the machine
    (preferably decorated with @BoundInnerClass from
    my "big" module, "pip3 install big").  You should
    also write a base class for your states with pure
    virtual methods for each of the events.

        class StateMachine:

            class State:

                @pure_virtual()
                def on_reset(*, verbose=False):
                    ...

    The body of the function is thrown away,
    and replaced with a raise RuntimeError
    warning about a pure abstract function call.
    """
    def pure_virtual(fn):
        def wrapper(self, *args, **kwargs):
            raise NotImplementedError(f"pure virtual method {fn.__name__} called")
        functools.update_wrapper(wrapper, fn)
        wrapper.__signature__ = inspect.signature(fn)
        return wrapper
    return pure_virtual


if __name__ == "__main__":
    import big.all as big
    import collections
    import sys
    import unittest

    class InterstateTests(unittest.TestCase):

        def test_on_exit_and_on_enter(self):
            methods_called = []

            @accessor()
            class StateMachine:
                def __init__(self, unittest):
                    self.unittest = unittest
                    self.interstate = Interstate(self.ZerothState(self, unittest), state_class=self.MyState, on_exit='on_my_exit')

                class MyState(State):
                    def __init__(self, sm, unittest):
                        self.sm = sm
                        self.unittest = unittest

                    def __repr__(self):
                        return f"<{type(self).__name__}>"

                class ZerothState(MyState):
                    pass

                class FirstState(MyState):
                    def on_enter(self):
                        self.unittest.assertEqual(self.sm.state, self)
                        self.unittest.assertEqual(self.sm.interstate.next, None)
                        methods_called.append('FirstState.on_enter')
                        sm.state = sm.SecondState(sm, self.unittest)

                class SecondState(MyState):
                    def on_my_exit(self):
                        self.unittest.assertEqual(self.sm.state, self)
                        self.unittest.assertEqual(self.sm.interstate.next, self.next)
                        methods_called.append('SecondState.on_exit')

                    def on_enter(self):
                        self.unittest.assertEqual(self.sm.state, self)
                        self.unittest.assertEqual(self.sm.interstate.next, None)
                        methods_called.append('SecondState.on_enter')
                        self.next = sm.ThirdState(sm, self.unittest)
                        sm.state = self.next

                class ThirdState(MyState):
                    def on_my_exit(self):
                        self.unittest.assertEqual(self.sm.state, self)
                        self.unittest.assertTrue(isinstance(self.sm.interstate.next, self.sm.FourthState))
                        methods_called.append('ThirdState.on_exit')

                class FourthState(MyState):
                    pass

            sm = StateMachine(self)
            sm.state = sm.FirstState(sm, self)
            sm.state = sm.FourthState(sm, self)

            self.assertEqual(methods_called,
                [
                'FirstState.on_enter',
                'SecondState.on_enter',
                'SecondState.on_exit',
                'ThirdState.on_exit',
                ])

            with self.assertRaises(TypeError):
                sm.state = None


        def test_methods_as_states(self):
            @accessor('st', 'inter')
            class MiamiStateMachine:
                def __init__(self):
                    self.inter = Interstate(self.false)

                def false(self):
                    return False

                def true(self):
                    return True

                def toggle(self):
                    self.st = self.true if self.st == self.false else self.false

            sm = MiamiStateMachine()
            self.assertIs(sm.st(), False)
            sm.toggle()
            self.assertIs(sm.st(), True)
            sm.toggle()
            self.assertIs(sm.st(), False)

            # test that state can be None.
            # I don't think it's a good idea, but Consenting Adults rules apply.
            sm.st = None
            self.assertIs(sm.st, None)


        def test_methods_as_values(self):
            @accessor()
            class MiamiStateMachine:
                def __init__(self):
                    self.interstate = Interstate(0)

                def increment(self):
                    self.state += 1

            sm = MiamiStateMachine()
            for i in range(20):
                self.assertIs(sm.state, i)
                sm.increment()


        def test_vending_machine(self):
            import decimal
            from decimal import Decimal

            decimal.getcontext().prec = 2
            Money = Decimal

            @accessor()
            class VendingMachine:
                """
                Super dumb vending machine.
                You wouldn't really design the code for a vending machine this way,
                this is just to exercise Interstate.

                Everything in the machine has the same price;
                that price is set by the "dollars" and "cents"
                arguments to the constructor.

                Because it's just a dumb example, and because this was easier,
                the machine doesn't auto-refund the remaining balance after
                vending.  You have to call refund() after vend() to get your
                money back.
                """
                def __init__(self, price):
                    self.balance = Money(0)
                    self.price = price

                    self.stock = collections.defaultdict(int)
                    self.not_ready = self.NotReadyToVend()
                    self.ready = self.ReadyToVend()
                    self.interstate = Interstate(self.not_ready,
                        state_class=self.VendingMachineState)

                def restock(self, product, count):
                    assert isinstance(count, int)
                    assert count > 0
                    self.stock[product] += count

                def in_stock(self, product):
                    return bool(self.stock[product])

                def status(self):
                    """
                    Returns a dict containing current status
                    """
                    return {
                        'balance': self.balance,
                        'stock': { product: count for product, count in self.stock.items() if count },
                        'ready': self.state == self.ready,
                    }

                def refund(self):
                    """
                    Refunds the machine's current balance.
                    Returns a Money object containing the remaining balance.
                    """
                    if not self.balance:
                        return Money(0)
                    amount = self.balance
                    self.balance = Money(0)
                    self.state.on_refund()
                    return amount

                def insert_money(self, money):
                    """
                    Adds money to the machine's current balance.
                    """
                    assert isinstance(money, Money)
                    assert money >= 0
                    self.balance += money
                    self.state.on_insert_money()

                @dispatch()
                def vend(self, product):
                    """
                    If you've put enough money in the machine,
                    and the requested product is in stock,
                    vends product.

                    Returns True on success and False for
                    failure.
                    """
                    ...

                @big.BoundInnerClass
                class VendingMachineState(State):

                    def __init__(self, machine):
                        self.machine = machine

                    def __repr__(self):
                        return f"<{type(self).__name__}>"

                    def on_refund(self):
                        pass

                    def on_insert_money(self):
                        pass

                    def on_vend(self, product):
                        pass

                @big.BoundInnerClass
                class NotReadyToVend(VendingMachineState.cls):
                    def on_insert_money(self):
                        machine = self.machine
                        if machine.balance >= machine.price:
                            machine.state = machine.ready

                    def vend(self, product):
                        return None

                @big.BoundInnerClass
                class ReadyToVend(VendingMachineState.cls):
                    def on_refund(self):
                        # balance is now zero
                        machine = self.machine
                        machine.state = machine.not_ready

                    def vend(self, product):
                        machine = self.machine
                        if not machine.stock[product]:
                            return None

                        machine.stock[product] -= 1
                        assert machine.stock[product] >= 0

                        machine.balance -= machine.price
                        assert machine.balance >= 0

                        if machine.balance < machine.price:
                            machine.state = machine.not_ready
                        return product

            quarter = Money(1) / Money(4)
            dime = Money(1) / Money(10)
            nickel = Money(1) / Money(20)

            vm = VendingMachine(quarter + quarter + nickel)

            rc_cola = 'Royal Crown Cola'
            vm.restock(rc_cola, 12)

            squirt = 'Squirt'
            vm.restock(squirt, 1)


            for round in range(1, 3):
                if round == 1:
                    self.assertEqual(vm.status(),
                        {
                        'balance': Money(0),
                        'stock': { rc_cola: 12, squirt: 1 },
                        'ready': False,
                        }
                        )
                elif round == 2:
                    self.assertEqual(vm.status(),
                        {
                        'balance': Money(0),
                        'stock': { rc_cola: 11 },
                        'ready': False,
                        }
                        )

                # not enough money yet, vending should fail
                result = vm.vend(rc_cola)
                self.assertEqual(result, None)

                vm.insert_money(quarter)
                vm.insert_money(quarter)
                vm.insert_money(nickel)

                # should vend one item now
                result = vm.vend(rc_cola)
                self.assertEqual(result, rc_cola)

                # ... but not two
                result = vm.vend(squirt)
                self.assertEqual(result, None)

                vm.insert_money(quarter)
                vm.insert_money(quarter)
                # still not enough money yet, vending should fail
                result = vm.vend(squirt)
                self.assertEqual(result, None)

                vm.insert_money(dime)
                result = vm.vend(squirt)
                change = vm.refund()

                if round == 1:
                    self.assertEqual(result, squirt)
                    self.assertEqual(change, nickel)
                elif round == 2:
                    self.assertEqual(result, None)
                    self.assertEqual(change, quarter + quarter + dime)

    unittest.main()