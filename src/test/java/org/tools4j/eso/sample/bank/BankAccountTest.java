/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 tools4j.org (Marco Terzer, Anton Anufriev)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.tools4j.eso.sample.bank;

import org.agrona.DirectBuffer;
import org.junit.Test;
import org.tools4j.eso.application.Application;
import org.tools4j.eso.application.SimpleApplication;
import org.tools4j.eso.command.Command;
import org.tools4j.eso.command.CommandLoopback;
import org.tools4j.eso.command.FlyweightCommand;
import org.tools4j.eso.event.Event;
import org.tools4j.eso.event.EventRouter;
import org.tools4j.eso.event.FlyweightEvent;
import org.tools4j.eso.sample.Context;
import org.tools4j.eso.sample.Launcher;
import org.tools4j.eso.sample.bank.actor.Accountant;
import org.tools4j.eso.sample.bank.actor.Teller;
import org.tools4j.eso.sample.bank.command.*;
import org.tools4j.eso.sample.bank.event.EventType;
import org.tools4j.eso.sample.bank.state.Bank;
import org.tools4j.eso.input.Input;
import org.tools4j.eso.log.InMemoryLog;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.util.Objects.requireNonNull;

public class BankAccountTest {

    private final Bank.Mutable bank = new Bank.Default();
    private final Teller teller = new Teller(bank);
    private final Accountant accountant = new Accountant(bank);
    private Application application = new SimpleApplication(
            "banking-app", this::process, this::apply
    );
    private Queue<BankCommand> commands = new ConcurrentLinkedQueue<>();
    private Input.Poller commandPoller = new CommandPoller(commands);

    @Test
    public void launch() throws Exception {
        commands.add(new CreateAccountCommand("Marco"));
        commands.add(new CreateAccountCommand("Henry"));
        commands.add(new CreateAccountCommand("Frank"));
        try (Launcher launcher = Launcher.launch(
                Context.create()
                        .application(application)
                        .input(666, commandPoller)
                        .output(this::publish)
                        .commandLog(new InMemoryLog<>(new FlyweightCommand()))
                        .eventLog(new InMemoryLog<>(new FlyweightEvent()))
        )) {
            //
            Thread.sleep(500);
            //WHEN: deposits
            commands.add(new DepositCommand("Marco", 1000.0));
            commands.add(new DepositCommand("Frank", 200.0));
            Thread.sleep(500);

            //WHEN: deposits + withdrawals
            commands.add(new DepositCommand("Marco", 200.0));
            commands.add(new WithdrawCommand("Frank", 50.0));
            Thread.sleep(500);

            //WHEN: illegal stuff
            commands.add(new WithdrawCommand("Henry", 1.0));
            commands.add(new DepositCommand("Lawry", 50.0));
            commands.add(new TransferCommand("Frank", "Marco", 200.0));

            //WHEN: other stuff that works
            commands.add(new CreateAccountCommand("Lawry"));
            commands.add(new DepositCommand("Lawry", 50.0));
            commands.add(new TransferCommand("Marco", "Frank", 100.0));
            commands.add(new WithdrawCommand("Frank", 200.0));

            //THEN: await termination
            while (!commands.isEmpty()) {
                launcher.join(20);
            }
            launcher.join(200);
            System.out.println("===========================================================");
        }
    }

    private void process(final Command command, final EventRouter router) {
        System.out.println("-----------------------------------------------------------");
        System.out.println("processing: " + command + ", payload=" + payloadFor(command));
        teller.onCommand(command, router);
    }

    private void apply(final Event event, final CommandLoopback commandLoopback) {
        System.out.println("applying: " + event + ", payload=" + payloadFor(event));
        accountant.onEvent(event, commandLoopback);
        if (event.isApplication()) {
            printBankAccounts(bank);
        }
    }

    private static void printBankAccounts(final Bank bank) {
        System.out.println("bank accounts:");
        for (final String account : bank.accounts()) {
            System.out.println("..." + account + ":\tbalance=" + bank.account(account).balance());

        }

    }

    private void publish(final Event event) {
        System.out.println("published: " + event + ", payload=" + payloadFor(event));
    }

    private String payloadFor(final Command command) {
        if (command.isApplication()) {
            return CommandType.toString(command);
        }
        return "(unknown)";
    }

    private String payloadFor(final Event event) {
        if (event.isApplication()) {
            return EventType.toString(event);
        }
        return "(unknown)";
    }

    private static class CommandPoller implements Input.Poller {
        final Queue<BankCommand> commands;
        long seq = 0;

        CommandPoller(final Queue<BankCommand> commands) {
            this.commands = requireNonNull(commands);
        }

        @Override
        public int poll(final Input.Handler handler) {
            final BankCommand cmd = commands.poll();
            if (cmd != null) {
                final int type = cmd.type().value;
                final DirectBuffer encoded = cmd.encode();
                handler.onMessage(++seq, type, encoded, 0, encoded.capacity());
                return 1;
            }
            return 0;
        }
    }
}