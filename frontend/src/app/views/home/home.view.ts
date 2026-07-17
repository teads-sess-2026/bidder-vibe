import { Component } from "@angular/core";
import { Note } from "../../types/note";
import { httpResource } from "@angular/common/http";
import { NOTES_BASE_URL } from "../../app.config";
import {FormsModule} from "@angular/forms";
import {RouterModule} from "@angular/router";

@Component({
    templateUrl: 'home.view.html',
    styleUrl: 'home.view.less',
    imports: [FormsModule, RouterModule],
})
export class HomeView {
    notes = httpResource<Note[]>(() => NOTES_BASE_URL, { defaultValue: [] });
}
