import { Component, inject, input, linkedSignal } from "@angular/core";
import { Router } from "@angular/router";
import { Note } from "../../types/note";
import { HttpClient, httpResource } from "@angular/common/http";
import { NOTES_BASE_URL } from "../../app.config";
import { form, FormField } from "@angular/forms/signals";

@Component({
    templateUrl: 'note.view.html',
    styleUrl: 'note.view.less',
    imports: [FormField],
})
export class NoteView {
    id = input<string>();

    private http = inject(HttpClient);
    private router = inject(Router);

    noteResource = httpResource<Note>(() =>
        this.id() && this.id() !== 'new' ? `${NOTES_BASE_URL}/${this.id()}` : undefined
    );

    note = linkedSignal<Note>(() =>
        this.noteResource.value() ?? { title: '', text: '' }
    );

    noteForm = form(this.note);

    saveChanges() {
        if (this.note().id) {
            this.http.put<Note>(
                `${NOTES_BASE_URL}/${this.note().id}`,
                this.note()
            ).subscribe(note => this.note.set(note));
        } else {
            this.http.post<Note>(
                NOTES_BASE_URL,
                this.note()
            ).subscribe(note => this.router.navigateByUrl(`/note/${note.id}`));
        }
    }

    deleteNote() {
        if (this.note().id && window.confirm("Are you sure you want to delete this note?")) {
            this.http.delete<void>(
                `${NOTES_BASE_URL}/${this.note().id}`,
            ).subscribe(() => this.router.navigateByUrl('/'));
        } else {
            this.router.navigateByUrl('/');
        }
    }
}
